package eventlog

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// TopicSpec is a topic EnsureTopics creates.
type TopicSpec struct {
	Name       string
	Partitions int
	// Fixed: the partition count must never change once the topic exists
	// (a command topic, whose partition picks which replica owns an
	// instance). A topic that isn't fixed is grown when it has fewer.
	Fixed bool
}

// EnsureTopics idempotently creates the topics, with replicationFactor
// copies of each partition (only applied at creation: an existing topic
// keeps its replication factor).
func EnsureTopics(brokers []string, replicationFactor int, topics ...TopicSpec) error {
	if len(brokers) == 0 {
		return fmt.Errorf("eventlog: no brokers configured")
	}

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("eventlog: dial %s: %w", brokers[0], err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("eventlog: find controller: %w", err)
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.Dial("tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("eventlog: dial controller %s: %w", controllerAddr, err)
	}
	defer controllerConn.Close()

	configs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		configs[i] = kafka.TopicConfig{Topic: t.Name, NumPartitions: t.Partitions, ReplicationFactor: replicationFactor}
	}
	// A no-op (not an error) for a topic that already exists, whatever its
	// partition count.
	if err := controllerConn.CreateTopics(configs...); err != nil {
		return fmt.Errorf("eventlog: create topics: %w", err)
	}

	// Asking to "increase" a topic to the count it already has is an
	// error, so only grow the ones that really have fewer.
	toBump := []kafka.TopicPartitionsConfig{}
	for _, t := range topics {
		partitions, err := conn.ReadPartitions(t.Name)
		if err != nil {
			return fmt.Errorf("eventlog: read partitions for %s: %w", t.Name, err)
		}
		switch {
		case len(partitions) == t.Partitions:
		case t.Fixed:
			return fmt.Errorf("eventlog: topic %s has %d partitions, %d configured: the partition count can't change once instances exist (keep KAFKA_PARTITIONS=%d)",
				t.Name, len(partitions), t.Partitions, len(partitions))
		case len(partitions) < t.Partitions:
			toBump = append(toBump, kafka.TopicPartitionsConfig{Name: t.Name, Count: int32(t.Partitions)})
		}
	}
	if len(toBump) == 0 {
		return nil
	}

	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	resp, err := client.CreatePartitions(context.Background(), &kafka.CreatePartitionsRequest{
		Addr:   kafka.TCP(controllerAddr),
		Topics: toBump,
	})
	if err != nil {
		return fmt.Errorf("eventlog: bump partition count: %w", err)
	}
	for topic, topicErr := range resp.Errors {
		if topicErr != nil {
			return fmt.Errorf("eventlog: bump partition count for %s: %w", topic, topicErr)
		}
	}
	return nil
}
