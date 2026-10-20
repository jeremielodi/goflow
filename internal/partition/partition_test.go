package partition

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"

	"github.com/jeremielodi/goflow/internal/cmdbus"
)

// A command lands on the partition cmdbus.PartitionOf gives its key: the
// mapping the ownership checks (cmdbus.OwnedHere) rely on.
func TestCommandsLandOnTheirKeysPartition(t *testing.T) {
	partitions := make([]int, 12)
	for i := range partitions {
		partitions[i] = i
	}
	for i := 0; i < 200; i++ {
		key := uuid.New()
		got := keyBalancer{}.Balance(kafka.Message{Key: []byte(key.String())}, partitions...)
		if want := cmdbus.PartitionOf(key, 12); got != want {
			t.Fatalf("key %s: partition %d, want %d", key, got, want)
		}
	}
}

func TestTransientErrors(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "08006"}, true},                                    // connection failure
		{fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "25006"}), true},         // read-only after a failover
		{&pgconn.PgError{Code: "40P01"}, true},                                    // deadlock
		{&pgconn.PgError{Code: "23505"}, false},                                   // unique violation: an answer
		{errors.New("dial tcp 10.0.0.5:5432: connect: connection refused"), true}, // database down
		{errors.New("job not found"), false},
	}
	for _, c := range cases {
		if got := transient(c.err); got != c.want {
			t.Errorf("transient(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
