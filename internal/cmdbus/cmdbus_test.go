package cmdbus

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

var errMissing = errors.New("missing")

type addArgs struct{ A, B int }

func registerAdd() {
	Register("test.add", func(ctx context.Context, raw json.RawMessage) (any, error) {
		var a addArgs
		if err := json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		if a.A < 0 {
			return nil, errMissing
		}
		return map[string]any{"sum": a.A + a.B, "inHandler": inHandler(ctx)}, nil
	})
	RegisterError("test_missing", errMissing)
}

// fakeTransport runs commands as a partition owner would: Execute, then
// the reply travels as JSON.
type fakeTransport struct {
	mu   sync.Mutex
	keys []uuid.UUID
}

func (f *fakeTransport) Send(ctx context.Context, cmd Command) (Reply, error) {
	f.mu.Lock()
	f.keys = append(f.keys, cmd.Key)
	f.mu.Unlock()
	reply, _ := Execute(context.Background(), cmd)
	raw, _ := json.Marshal(reply)
	var back Reply
	err := json.Unmarshal(raw, &back)
	return back, err
}

func (f *fakeTransport) Post(ctx context.Context, cmd Command) error {
	_, err := f.Send(ctx, cmd)
	return err
}

func TestDirectAndPartitionedGiveTheSameOutcome(t *testing.T) {
	registerAdd()
	key := uuid.New()
	for _, partitioned := range []bool{false, true} {
		ft := &fakeTransport{}
		if partitioned {
			SetTransport(ft)
		} else {
			SetTransport(nil)
		}

		var out struct {
			Sum       int  `json:"sum"`
			InHandler bool `json:"inHandler"`
		}
		if err := Do(context.Background(), Key(key), "test.add", addArgs{2, 3}, &out); err != nil {
			t.Fatalf("partitioned=%v: %v", partitioned, err)
		}
		if out.Sum != 5 || !out.InHandler {
			t.Fatalf("partitioned=%v: got %+v", partitioned, out)
		}

		err := Do(context.Background(), Key(key), "test.add", addArgs{-1, 0}, nil)
		if !errors.Is(err, errMissing) || err.Error() != "missing" {
			t.Fatalf("partitioned=%v: error %v doesn't match its sentinel", partitioned, err)
		}

		if partitioned && (len(ft.keys) != 2 || ft.keys[0] != key) {
			t.Fatalf("commands not keyed by their instance: %v", ft.keys)
		}
	}
	SetTransport(nil)
}

func TestACommandFromAHandlerRunsInPlace(t *testing.T) {
	registerAdd()
	ft := &fakeTransport{}
	SetTransport(ft)
	defer SetTransport(nil)

	if err := Do(InHandler(context.Background()), Key(uuid.New()), "test.add", addArgs{1, 1}, nil); err != nil {
		t.Fatal(err)
	}
	if len(ft.keys) != 0 {
		t.Fatal("a command issued by a handler must not wait on a partition")
	}
}

func TestKeyResolvedOnlyWhenPartitioned(t *testing.T) {
	registerAdd()
	SetTransport(nil)
	called := false
	lookup := func() (uuid.UUID, error) { called = true; return uuid.New(), nil }
	if err := Do(context.Background(), lookup, "test.add", addArgs{1, 1}, nil); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("direct mode must not pay for resolving the partition key")
	}
}

func TestPartitionOfIsStableAndSpread(t *testing.T) {
	key := uuid.MustParse("00000000-0000-0005-0000-000000000000")
	if got := PartitionOf(key, 12); got != 5 {
		t.Fatalf("PartitionOf = %d, want 5 (first 8 bytes, big-endian, modulo n)", got)
	}
	seen := map[int]bool{}
	for i := 0; i < 2000; i++ {
		seen[PartitionOf(uuid.New(), 12)] = true
	}
	if len(seen) != 12 {
		t.Fatalf("random keys reached %d of 12 partitions", len(seen))
	}
	if NameKey("message", "a", "b") != NameKey("message", "a", "b") || NameKey("message", "ab", "") == NameKey("message", "a", "b") {
		t.Fatal("NameKey must be stable and keep its parts apart")
	}
}
