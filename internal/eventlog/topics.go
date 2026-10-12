package eventlog

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

// EnsureTopics idempotently creates Topic (goflow.process-events) and
// CommandsTopic (goflow.process-commands) with numPartitions partitions
// each and replicationFactor copies of each partition (only applied at
// creation: an existing topic keeps its replication factor), bumping
// Topic's partition count up if it already exists with fewer
// — which it will the first time this runs after Phase 0, since that
// shadow-mode topic was auto-created with Redpanda/Kafka's default of 1
// partition (Phase 0 never needed partition-key affinity to matter for
// correctness the way Phase 1's partition ownership does).
//
// Must run before any process definition is ever flipped to the
// partitioned write path (see main.go) — bumping an existing topic's
// partition count changes its future key->partition mapping, which is
// only safe here because no partitioned-write-path instance exists yet to
// have that mapping change out from under it. See
// architecture/event-sourced-kafka-design.md for the full reasoning.
func EnsureTopics(brokers []string, numPartitions, replicationFactor int) error {
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

	if err := controllerConn.CreateTopics(
		kafka.TopicConfig{Topic: Topic, NumPartitions: numPartitions, ReplicationFactor: replicationFactor},
		kafka.TopicConfig{Topic: CommandsTopic, NumPartitions: numPartitions, ReplicationFactor: replicationFactor},
	); err != nil {
		return fmt.Errorf("eventlog: create topics: %w", err)
	}

	// CreateTopics is a no-op (not an error) for a topic that already
	// exists, regardless of its current partition count, so check each
	// topic's *actual* current count before ever asking to bump it —
	// asking Kafka/Redpanda to "increase" a topic to a count it's already
	// at (or above) is a hard error (INVALID_REQUEST here, not the more
	// specific INVALID_PARTITION_INCREASE code some brokers return), not
	// a benign no-op like CreateTopics's redeploy case.
	toBump := []kafka.TopicPartitionsConfig{}
	for _, topic := range []string{Topic, CommandsTopic} {
		partitions, err := conn.ReadPartitions(topic)
		if err != nil {
			return fmt.Errorf("eventlog: read partitions for %s: %w", topic, err)
		}
		if len(partitions) < numPartitions {
			toBump = append(toBump, kafka.TopicPartitionsConfig{Name: topic, Count: int32(numPartitions)})
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
