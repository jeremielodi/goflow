package partition

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"

	"github.com/jeremielodi/goflow/internal/cmdbus"
)

// GroupID is the consumer group every replica's Processor joins: Kafka
// hands each partition to exactly one member, and moves it when a member
// leaves or joins.
const GroupID = "goflow-partition-owners"

// Processor owns the partitions Kafka assigns to this replica and runs
// their commands, in order, one at a time per partition.
type Processor struct {
	brokers    []string
	group      *kafka.ConsumerGroup
	replies    *kafka.Writer
	partitions int

	mu    sync.RWMutex
	owned map[int]bool
}

func NewProcessor(brokers []string, partitions int) (*Processor, error) {
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      GroupID,
		Brokers: brokers,
		Topics:  []string{CommandsTopic},
		Dialer:  &kafka.Dialer{ClientID: clientID(), Timeout: 10 * time.Second, DualStack: true},
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Printf("partition owner: kafka: "+msg, args...)
		}),
		// A stopped replica's partitions move to the others within this.
		SessionTimeout:    6 * time.Second,
		HeartbeatInterval: time.Second,
		RebalanceTimeout:  30 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	p := &Processor{brokers: brokers, group: group, replies: newWriter(brokers, RepliesTopic), partitions: partitions, owned: map[int]bool{}}
	cmdbus.SetOwnership(p.owns)
	return p, nil
}

// owns reports whether this replica currently owns key's partition.
func (p *Processor) owns(key uuid.UUID) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.owned[cmdbus.PartitionOf(key, p.partitions)]
}

func (p *Processor) setOwned(partition int, owned bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if owned {
		p.owned[partition] = true
	} else {
		delete(p.owned, partition)
	}
}

// Run takes part in the group until ctx ends: for every partition this
// replica gets, a goroutine runs its commands (see own).
func (p *Processor) Run(ctx context.Context) {
	defer p.group.Close()
	defer p.replies.Close()
	for {
		gen, err := p.group.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("partition owner: joining the group: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		owned := make([]int, 0, len(gen.Assignments[CommandsTopic]))
		for _, a := range gen.Assignments[CommandsTopic] {
			owned = append(owned, a.ID+1)
		}
		log.Printf("partition owner: generation %d, owning partitions %v", gen.ID, owned)
		for _, assignment := range gen.Assignments[CommandsTopic] {
			partition, offset := assignment.ID, assignment.Offset
			gen.Start(func(genCtx context.Context) {
				p.own(genCtx, gen, partition, offset)
			})
		}
	}
}

// own runs one partition's commands for as long as this replica owns it.
// A command being run when the partition is taken away (genCtx ends)
// finishes first: Kafka gives the partition to its next owner only once
// this returns, so two replicas never run commands of one partition at the
// same time.
func (p *Processor) own(genCtx context.Context, gen *kafka.Generation, partition int, offset int64) {
	p.setOwned(partition, true)
	defer p.setOwned(partition, false)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   p.brokers,
		Topic:     CommandsTopic,
		Partition: partition,
		MaxWait:   50 * time.Millisecond,
	})
	defer reader.Close()
	if offset >= 0 {
		reader.SetOffset(offset)
	} else {
		reader.SetOffset(kafka.FirstOffset)
	}

	// The first command read after taking a partition over may have been
	// run already: its previous owner died before acknowledging it.
	first := true
	for {
		msg, err := reader.FetchMessage(genCtx)
		if err != nil {
			if genCtx.Err() != nil {
				return
			}
			log.Printf("partition owner: partition %d: %v", partition, err)
			time.Sleep(time.Second)
			continue
		}

		var cmd cmdbus.Command
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			log.Printf("partition owner: partition %d offset %d: malformed command, skipped: %v", partition, msg.Offset, err)
		} else if !p.run(genCtx, partition, cmd, first) {
			return // partition taken away while retrying: its next owner runs it
		}
		first = false

		if err := gen.CommitOffsets(map[string]map[int]int64{CommandsTopic: {partition: msg.Offset + 1}}); err != nil {
			log.Printf("partition owner: partition %d: acknowledging offset %d: %v", partition, msg.Offset, err)
		}
	}
}

// run executes cmd and replies to whoever waits. An error that is an
// answer (job not found…) is replied; one meaning the database is out of
// reach is retried, as a redelivery, until it works or the partition goes
// to another replica (false).
func (p *Processor) run(genCtx context.Context, partition int, cmd cmdbus.Command, redelivered bool) bool {
	delay := 200 * time.Millisecond
	for {
		ctx := context.Background()
		if redelivered {
			ctx = cmdbus.WithRedelivered(ctx)
		}
		reply, err := cmdbus.Execute(ctx, cmd)
		if err == nil || !transient(err) {
			p.reply(cmd, reply)
			return true
		}
		log.Printf("partition owner: partition %d: %s failed, retrying in %s: %v", partition, cmd.Op, delay, err)
		select {
		case <-genCtx.Done():
			return false
		case <-time.After(delay):
		}
		redelivered = true
		if delay < 5*time.Second {
			delay *= 2
		}
	}
}

func (p *Processor) reply(cmd cmdbus.Command, reply cmdbus.Reply) {
	if cmd.ReplyTo == "" {
		if reply.Error != "" {
			log.Printf("partition owner: %s for %s: %s", cmd.Op, cmd.Key, reply.Error)
		}
		return
	}
	value, err := json.Marshal(reply)
	if err != nil {
		log.Printf("partition owner: encoding the reply to %s: %v", cmd.Op, err)
		return
	}
	msg := kafka.Message{Key: []byte(replyKey(cmd.ReplyTo).String()), Value: value}
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = p.replies.WriteMessages(ctx, msg)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("partition owner: replying to %s: %v", cmd.Op, err)
}

// transient tells an error worth retrying the same command for: the
// database is unreachable, restarting, failing over, or picked this
// transaction to abort.
func transient(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"), // connection exception
			pgErr.Code == "57P01", pgErr.Code == "57P02", pgErr.Code == "57P03", // shutdown, cannot connect now
			pgErr.Code == "25006",                        // read-only: a replica promoted away from us
			pgErr.Code == "40001", pgErr.Code == "40P01": // serialization failure, deadlock
			return true
		}
		return false
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") || strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset") || strings.Contains(msg, "bad connection")
}
