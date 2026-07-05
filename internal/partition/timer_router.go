package partition

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/jeremielodi/goflow/internal/eventlog"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/service"
)

// TimerRouter implements service.TimerPartitionRouter — see that
// interface's doc comment in internal/service/timer_scheduler.go for why
// this indirection exists (internal/service cannot import
// internal/partition directly). Wired only in main.go.
type TimerRouter struct {
	db *sqlx.DB
}

func NewTimerRouter(db *sqlx.DB) *TimerRouter {
	return &TimerRouter{db: db}
}

// FireTimer produces a TimerFiredCommand for a partitioned-write-path
// instance's due timer instead of letting TimerScheduler resume it
// directly. Unlike every other bridge function in this package, it never
// waits for a confirmation event — see the interface doc comment for why.
func (r *TimerRouter) FireTimer(ctx context.Context, timer models.TimerJob) (bool, error) {
	writePath, err := LookupInstanceOwner(r.db, timer.ProcessInstanceID)
	if err != nil || writePath != "partitioned" {
		// Any lookup error (including "not found") is treated as "not
		// partitioned" — the legacy path is always a safe fallback and
		// already handles instances this router can't resolve.
		return false, nil
	}

	cmdProducer := eventlog.GlobalCommand()
	if cmdProducer == nil {
		return false, fmt.Errorf("partitioned write path is not configured on this instance")
	}

	if timer.ExecutionID == nil {
		return false, fmt.Errorf("timer %s has no execution id", timer.ID)
	}
	targetNodeID, err := service.ParseTimerTargetNode(timer.Payload)
	if err != nil {
		return false, err
	}

	if err := cmdProducer.FireTimer(ctx, timer.ProcessInstanceID, timer.ID, *timer.ExecutionID, targetNodeID); err != nil {
		return false, err
	}
	return true, nil
}
