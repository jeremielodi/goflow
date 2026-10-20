// Package cmdbus routes every command that changes a process instance —
// start it, complete a job or a user task, set variables, fire a timer,
// correlate a message, cancel it… — to the one place allowed to run it.
//
// Two modes, chosen once at boot (WRITE_PATH):
//
//   - direct (default): the command runs right here, in the caller's
//     goroutine. Concurrency between replicas is handled by Postgres row
//     locks, as it always has been.
//   - partitioned: like Camunda 8's partitions. Every command is keyed by
//     its process instance; the key picks a Kafka partition, and the one
//     replica that owns that partition runs it, one command after the
//     other. The caller waits for the reply (or doesn't, for Post).
//
// The handler code is the same in both modes: a command is registered once
// (Register) and invoked through Do/Post, which only decide where it runs.
// Arguments and results go through JSON in both modes, so a handler that
// works directly works partitioned.
package cmdbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

// Handler runs one command. args is the JSON of what the caller passed;
// the result is marshalled to JSON for the caller.
type Handler func(ctx context.Context, args json.RawMessage) (any, error)

// Command is what travels to the partition owner.
type Command struct {
	ID      uuid.UUID       `json:"id"`
	Key     uuid.UUID       `json:"key"`
	Op      string          `json:"op"`
	Args    json.RawMessage `json:"args"`
	ReplyTo string          `json:"replyTo,omitempty"` // empty: nobody waits
}

// Reply is the outcome of a command, sent back to whoever waits for it.
type Reply struct {
	ID        uuid.UUID       `json:"id"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	ErrorCode string          `json:"errorCode,omitempty"`
}

// Transport carries commands to their partition owner. nil means direct
// mode.
type Transport interface {
	// Send delivers cmd and waits for its reply.
	Send(ctx context.Context, cmd Command) (Reply, error)
	// Post delivers cmd without waiting for it to run.
	Post(ctx context.Context, cmd Command) error
}

var (
	mu        sync.RWMutex
	handlers  = map[string]Handler{}
	transport Transport
	errCodes  []codedError
)

type codedError struct {
	code string
	err  error
}

// Register makes op runnable. Registering the same op twice replaces it
// (handlers are set up once at boot).
func Register(op string, h Handler) {
	mu.Lock()
	defer mu.Unlock()
	handlers[op] = h
}

// RegisterError lets callers keep testing a sentinel error with errors.Is
// when the command ran on another replica: the error travels as code and
// comes back wrapping err.
func RegisterError(code string, err error) {
	mu.Lock()
	defer mu.Unlock()
	for _, c := range errCodes {
		if c.code == code {
			return
		}
	}
	errCodes = append(errCodes, codedError{code, err})
}

// SetTransport switches to partitioned mode (nil: back to direct).
func SetTransport(t Transport) {
	mu.Lock()
	defer mu.Unlock()
	transport = t
}

// Partitioned reports whether commands go through partitions.
func Partitioned() bool {
	mu.RLock()
	defer mu.RUnlock()
	return transport != nil
}

type inHandlerKey struct{}

// InHandler marks ctx as running inside a command handler.
func InHandler(ctx context.Context) context.Context {
	return context.WithValue(ctx, inHandlerKey{}, true)
}

func inHandler(ctx context.Context) bool {
	v, _ := ctx.Value(inHandlerKey{}).(bool)
	return v
}

// KeyFunc resolves the process instance a command is about. It is only
// called in partitioned mode, so a lookup costs nothing in direct mode.
type KeyFunc func() (uuid.UUID, error)

// Key is a KeyFunc for an already known instance.
func Key(id uuid.UUID) KeyFunc { return func() (uuid.UUID, error) { return id, nil } }

// Do runs op with args and waits for it, decoding its result into result
// (a pointer, or nil to ignore it).
//
// A command issued from inside a handler runs in place: the handler
// already owns its partition, and waiting on another one (or its own) from
// there could deadlock.
func Do(ctx context.Context, key KeyFunc, op string, args any, result any) error {
	t, h, err := lookup(op)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("%s: encode arguments: %w", op, err)
	}
	if t == nil || inHandler(ctx) {
		return runLocal(ctx, h, raw, result)
	}
	k, err := key()
	if err != nil {
		return err
	}
	reply, err := t.Send(ctx, Command{ID: uuid.New(), Key: k, Op: op, Args: raw})
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return decodeReply(reply, result)
}

// Post runs op without waiting for its outcome (timers, conditions…).
// In direct mode, or from inside a handler, it runs in place, right away.
func Post(ctx context.Context, key KeyFunc, op string, args any) error {
	t, h, err := lookup(op)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(args)
	if err != nil {
		return fmt.Errorf("%s: encode arguments: %w", op, err)
	}
	if t == nil || inHandler(ctx) {
		return runLocal(ctx, h, raw, nil)
	}
	k, err := key()
	if err != nil {
		return err
	}
	return t.Post(ctx, Command{ID: uuid.New(), Key: k, Op: op, Args: raw})
}

func lookup(op string) (Transport, Handler, error) {
	mu.RLock()
	defer mu.RUnlock()
	h, ok := handlers[op]
	if !ok {
		return nil, nil, fmt.Errorf("cmdbus: unknown command %q", op)
	}
	return transport, h, nil
}

func runLocal(ctx context.Context, h Handler, args json.RawMessage, result any) error {
	out, err := h(InHandler(ctx), args)
	if err != nil {
		return err
	}
	return decodeResult(out, result)
}

func decodeResult(out any, result any) error {
	if result == nil || out == nil {
		return nil
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	return json.Unmarshal(raw, result)
}

// Execute runs cmd on this replica — what the partition owner does with a
// command it reads — and builds the reply. The error is the handler's own,
// for the owner to tell a failure worth retrying (database unreachable)
// from an answer (job not found).
func Execute(ctx context.Context, cmd Command) (Reply, error) {
	reply := Reply{ID: cmd.ID}
	_, h, err := lookup(cmd.Op)
	if err == nil {
		var out any
		out, err = h(InHandler(ctx), cmd.Args)
		if err == nil && out != nil {
			reply.Result, err = json.Marshal(out)
		}
	}
	if err != nil {
		reply.Error = err.Error()
		reply.ErrorCode = codeOf(err)
	}
	return reply, err
}

func codeOf(err error) string {
	mu.RLock()
	defer mu.RUnlock()
	for _, c := range errCodes {
		if errors.Is(err, c.err) {
			return c.code
		}
	}
	return ""
}

func decodeReply(reply Reply, result any) error {
	if reply.Error != "" {
		return &RemoteError{Message: reply.Error, sentinel: sentinelOf(reply.ErrorCode)}
	}
	if result == nil || len(reply.Result) == 0 {
		return nil
	}
	return json.Unmarshal(reply.Result, result)
}

func sentinelOf(code string) error {
	if code == "" {
		return nil
	}
	mu.RLock()
	defer mu.RUnlock()
	for _, c := range errCodes {
		if c.code == code {
			return c.err
		}
	}
	return nil
}

// RemoteError is a handler's error as it comes back from another replica:
// same message, and errors.Is still matches its registered sentinel.
type RemoteError struct {
	Message  string
	sentinel error
}

func (e *RemoteError) Error() string { return e.Message }
func (e *RemoteError) Unwrap() error { return e.sentinel }

type redeliveredKey struct{}

// WithRedelivered marks ctx as running a command that may already have
// been applied: the partition owner died before acknowledging it.
func WithRedelivered(ctx context.Context) context.Context {
	return context.WithValue(ctx, redeliveredKey{}, true)
}

// Redelivered reports whether the running command may already have been
// applied. A handler finding its change already there then completes
// whatever the dead owner left unfinished instead of failing.
func Redelivered(ctx context.Context) bool {
	v, _ := ctx.Value(redeliveredKey{}).(bool)
	return v
}

var nameSpace = uuid.MustParse("6f1d3a52-6d5b-4a3e-9a62-0c1f4f0e7a11")

// NameKey is the partition key of a command that isn't about one process
// instance (a message, keyed by its correlation key; a signal, by name).
func NameKey(parts ...string) uuid.UUID {
	name := ""
	for i, p := range parts {
		if i > 0 {
			name += "\x00"
		}
		name += p
	}
	return uuid.NewSHA1(nameSpace, []byte(name))
}

// PartitionOf is the partition (0..n-1) owning key: its first 8 bytes,
// big-endian, modulo n.
func PartitionOf(key uuid.UUID, n int) int {
	var v uint64
	for _, b := range key[:8] {
		v = v<<8 | uint64(b)
	}
	return int(v % uint64(n))
}

var ownedHere func(uuid.UUID) bool

// SetOwnership tells which keys this replica currently owns the partition
// of (set by the partition owner in partitioned mode).
func SetOwnership(f func(uuid.UUID) bool) {
	mu.Lock()
	defer mu.Unlock()
	ownedHere = f
}

// OwnedHere reports whether key's commands run on this replica — always
// true in direct mode. Background loops present on every replica (a
// condition re-check, a kept message) use it so only one of them issues
// the command.
func OwnedHere(key uuid.UUID) bool {
	mu.RLock()
	t, f := transport, ownedHere
	mu.RUnlock()
	if t == nil || f == nil {
		return true
	}
	return f(key)
}
