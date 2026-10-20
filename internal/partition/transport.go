// Package partition runs GoFlow the way Camunda 8 runs Zeebe: every
// command about a process instance goes to the Kafka partition of that
// instance, and the one replica owning the partition runs it — commands
// of one instance never run concurrently, whichever replica received
// them. See package cmdbus for what a command is; this package is its
// Kafka transport (Transport, the gateway side) and the partition owner
// (Processor).
//
// Postgres stays the state: a partition owner keeps nothing in memory, so
// when a partition moves to another replica (one stopped, one joined) the
// new owner just carries on from the last acknowledged command.
package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/jeremielodi/goflow/internal/cmdbus"
)

const (
	// CommandsTopic holds the commands, one partition per owner slot. Its
	// partition count never changes once created.
	CommandsTopic = "goflow.partition-commands"
	// RepliesTopic carries outcomes back to the replica waiting for them.
	RepliesTopic = "goflow.partition-replies"
)

// keyBalancer sends a message to the partition its key (a uuid) maps to,
// cmdbus.PartitionOf — the same mapping wherever it's computed.
type keyBalancer struct{}

func (keyBalancer) Balance(msg kafka.Message, partitions ...int) int {
	key, err := uuid.ParseBytes(msg.Key)
	if err != nil || len(partitions) == 0 {
		return partitions[0]
	}
	return partitions[cmdbus.PartitionOf(key, len(partitions))]
}

func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     keyBalancer{},
		RequiredAcks: kafka.RequireAll,
		// A command waits for its write: send at once, batching only what
		// is written concurrently.
		BatchTimeout: time.Millisecond,
		BatchSize:    500,
	}
}

// Transport is cmdbus's partitioned mode, on the side sending commands.
type Transport struct {
	commands *kafka.Writer
	replies  *kafka.Reader
	replica  string
	timeout  time.Duration

	mu      sync.Mutex
	pending map[uuid.UUID]chan cmdbus.Reply
}

// NewTransport sends commands through Kafka and reads this replica's
// replies. timeout bounds how long a caller waits for a command's outcome
// (the command may still run after that: like a Zeebe request deadline).
func NewTransport(ctx context.Context, brokers []string, partitions int, timeout time.Duration) *Transport {
	replica := uuid.New().String()
	t := &Transport{
		commands: newWriter(brokers, CommandsTopic),
		replica:  replica,
		timeout:  timeout,
		pending:  map[uuid.UUID]chan cmdbus.Reply{},
	}
	// The replies addressed to this replica land on one partition of the
	// replies topic; read it from its end.
	t.replies = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     RepliesTopic,
		Partition: replyPartition(replica, partitions),
		MaxWait:   50 * time.Millisecond,
	})
	if err := t.replies.SetOffset(kafka.LastOffset); err != nil {
		log.Printf("partition transport: %v", err)
	}
	go t.readReplies(ctx)
	return t
}

func replyKey(replica string) uuid.UUID { return cmdbus.NameKey("replica", replica) }

func replyPartition(replica string, partitions int) int {
	return cmdbus.PartitionOf(replyKey(replica), partitions)
}

// Send implements cmdbus.Transport.
func (t *Transport) Send(ctx context.Context, cmd cmdbus.Command) (cmdbus.Reply, error) {
	cmd.ReplyTo = t.replica
	ch := make(chan cmdbus.Reply, 1)
	t.mu.Lock()
	t.pending[cmd.ID] = ch
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		delete(t.pending, cmd.ID)
		t.mu.Unlock()
	}()

	if err := t.write(ctx, cmd); err != nil {
		return cmdbus.Reply{}, err
	}
	timer := time.NewTimer(t.timeout)
	defer timer.Stop()
	select {
	case reply := <-ch:
		return reply, nil
	case <-timer.C:
		return cmdbus.Reply{}, fmt.Errorf("no answer from the owner of its partition within %s (it may still run)", t.timeout)
	case <-ctx.Done():
		return cmdbus.Reply{}, ctx.Err()
	}
}

// Post implements cmdbus.Transport.
func (t *Transport) Post(ctx context.Context, cmd cmdbus.Command) error {
	return t.write(ctx, cmd)
}

func (t *Transport) write(ctx context.Context, cmd cmdbus.Command) error {
	value, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	// Not the caller's context: a request cancelled mid-write must not
	// leave a half-sent command it believes was never sent.
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := t.commands.WriteMessages(wctx, kafka.Message{Key: []byte(cmd.Key.String()), Value: value}); err != nil {
		return fmt.Errorf("send to its partition: %w", err)
	}
	return nil
}

func (t *Transport) readReplies(ctx context.Context) {
	defer t.replies.Close()
	for {
		msg, err := t.replies.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("partition transport: reading replies: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if string(msg.Key) != replyKey(t.replica).String() {
			continue // for another replica sharing this partition
		}
		var reply cmdbus.Reply
		if err := json.Unmarshal(msg.Value, &reply); err != nil {
			log.Printf("partition transport: malformed reply: %v", err)
			continue
		}
		t.mu.Lock()
		ch := t.pending[reply.ID]
		t.mu.Unlock()
		if ch != nil {
			select {
			case ch <- reply:
			default: // a duplicate (redelivered command)
			}
		}
	}
}

// Close stops sending.
func (t *Transport) Close() error {
	return errors.Join(t.commands.Close())
}
