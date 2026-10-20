package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/jeremielodi/goflow/internal/cmdbus"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/service"
)

// The commands that change a process instance from outside — an API call,
// a worker, a timer, a variable change. Each runs through cmdbus: directly
// in the caller, or on the replica owning the instance's partition (see
// package cmdbus). Their bodies are what the HTTP and gRPC handlers used
// to do inline.
const (
	CmdStartInstance    = "StartInstance"
	CmdCompleteJob      = "CompleteJob"
	CmdFailJob          = "FailJob"
	CmdSetJobRetries    = "SetJobRetries"
	CmdResolveIncident  = "ResolveIncident"
	CmdTerminate        = "TerminateInstance"
	CmdCompleteUserTask = "CompleteUserTask"
	CmdSetVariables     = "SetVariables"
	CmdModify           = "ModifyInstance"
	CmdPublishMessage   = "PublishMessage"
	CmdBroadcastSignal  = "BroadcastSignal"
	CmdFireStartTimer   = "FireStartTimer"
	CmdCheckConditions  = "CheckConditions"
)

// ErrExecutionFailed wraps an error raised while running the process
// after a command was applied (the command itself is durable).
var ErrExecutionFailed = errors.New("execution failed")

type commands struct {
	db         *sqlx.DB
	dispatcher *events.TaskEventDispatcher
	tasks      *service.TaskService
}

var cmds *commands

// RegisterCommands makes the engine's commands runnable on this replica.
// Every replica registers them at boot: in partitioned mode any of them
// may own the partition a command lands on.
func RegisterCommands(db *sqlx.DB, dispatcher *events.TaskEventDispatcher, tasks *service.TaskService) {
	cmds = &commands{db: db, dispatcher: dispatcher, tasks: tasks}

	for code, err := range map[string]error{
		"job_not_found":             service.ErrJobNotFound,
		"job_invalid_state":         service.ErrJobInvalidState,
		"instance_not_found":        service.ErrProcessInstanceNotFound,
		"instance_already_started":  service.ErrProcessAlreadyStarted,
		"timer_stale":               service.ErrStaleTimer,
		"message_already_published": ErrMessageAlreadyPublished,
		"execution_failed":          ErrExecutionFailed,
		"incident_not_found":        ErrIncidentNotFound,
		"task_not_found":            ErrUserTaskNotFound,
	} {
		cmdbus.RegisterError(code, err)
	}

	register(CmdStartInstance, cmds.startInstance)
	register(CmdCompleteJob, cmds.completeJob)
	register(CmdFailJob, cmds.failJob)
	register(CmdSetJobRetries, cmds.setJobRetries)
	register(CmdResolveIncident, cmds.resolveIncident)
	register(CmdTerminate, cmds.terminate)
	register(CmdCompleteUserTask, cmds.completeUserTask)
	register(CmdSetVariables, cmds.setVariables)
	register(CmdModify, cmds.modify)
	register(CmdPublishMessage, cmds.publishMessage)
	register(CmdBroadcastSignal, cmds.broadcastSignal)
	register(CmdFireStartTimer, cmds.fireStartTimer)
	register(CmdCheckConditions, cmds.checkConditions)
	register(CmdDeliverKeptMessage, cmds.deliverKeptMessage)
}

// register adapts a typed handler to cmdbus.
func register[A any, R any](op string, h func(ctx context.Context, args A) (R, error)) {
	cmdbus.Register(op, func(ctx context.Context, raw json.RawMessage) (any, error) {
		var args A
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &args); err != nil {
				return nil, fmt.Errorf("%s: decode arguments: %w", op, err)
			}
		}
		return h(ctx, args)
	})
}

// instanceOf returns a cmdbus.KeyFunc reading a row's process instance.
func instanceOf(db *sqlx.DB, table string, id uuid.UUID, notFound error) cmdbus.KeyFunc {
	return func() (uuid.UUID, error) {
		var instanceID uuid.UUID
		if err := db.Get(&instanceID, `SELECT process_instance_id FROM public.`+table+` WHERE id = $1`, id); err != nil {
			return uuid.Nil, fmt.Errorf("%w: %s", notFound, id)
		}
		return instanceID, nil
	}
}

// resumeStranded runs again every execution of the instance left 'active'
// — at rest, an execution is 'waiting' at a wait state or finished, so
// 'active' means a run was cut short (the replica died mid-run). Called
// when a command delivered again after such a crash finds its own change
// already applied.
func (c *commands) resumeStranded(ctx context.Context, instanceID uuid.UUID) error {
	var ids []uuid.UUID
	if err := c.db.Select(&ids, `
		SELECT id FROM public.executions
		WHERE process_instance_id = $1 AND status = 'active'
		ORDER BY created_at`, instanceID); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	graph, err := repository.NewEngineRepository(c.db, c.dispatcher).GetProcessGraphByInstanceID(instanceID)
	if err != nil {
		return err
	}
	rt := NewRuntime(graph, c.db, c.dispatcher)
	for _, id := range ids {
		if err := rt.ExecuteExecution(ctx, id); err != nil {
			return fmt.Errorf("%w: %w", ErrExecutionFailed, err)
		}
	}
	return nil
}

// ── Start ───────────────────────────────────────────────────────────────

type StartInstanceArgs struct {
	DefinitionID uuid.UUID              `json:"definitionId"`
	InstanceID   uuid.UUID              `json:"instanceId"`
	ExecutionID  uuid.UUID              `json:"executionId"`
	TenantID     *string                `json:"tenantId,omitempty"`
	Variables    map[string]interface{} `json:"variables,omitempty"`
	BusinessKey  string                 `json:"businessKey,omitempty"`
}

// StartInstance creates an instance of a process definition and runs it
// up to its first wait states. The ids are chosen here, before the
// command travels: the instance id is its partition key.
func StartInstance(ctx context.Context, definitionID uuid.UUID, tenantID *string, variables map[string]interface{}) (instanceID, execID uuid.UUID, err error) {
	return StartInstanceWithBusinessKey(ctx, definitionID, tenantID, variables, "")
}

// StartInstanceWithBusinessKey is StartInstance with a Camunda 7 business
// key.
func StartInstanceWithBusinessKey(ctx context.Context, definitionID uuid.UUID, tenantID *string, variables map[string]interface{}, businessKey string) (instanceID, execID uuid.UUID, err error) {
	args := StartInstanceArgs{
		DefinitionID: definitionID,
		InstanceID:   uuid.New(),
		ExecutionID:  uuid.New(),
		TenantID:     tenantID,
		Variables:    variables,
		BusinessKey:  businessKey,
	}
	err = cmdbus.Do(ctx, cmdbus.Key(args.InstanceID), CmdStartInstance, args, nil)
	return args.InstanceID, args.ExecutionID, err
}

func (c *commands) startInstance(ctx context.Context, a StartInstanceArgs) (struct{}, error) {
	graph, err := repository.NewEngineRepository(c.db, c.dispatcher).GetProcessGraphByDefinitionID(a.DefinitionID)
	if err != nil {
		return struct{}{}, fmt.Errorf("load process definition %s: %w", a.DefinitionID, err)
	}
	err = service.StartProcessInstanceWithIDs(c.db, graph, a.DefinitionID, a.InstanceID, a.ExecutionID, a.TenantID, a.Variables, a.BusinessKey)
	if errors.Is(err, service.ErrProcessAlreadyStarted) && cmdbus.Redelivered(ctx) {
		return struct{}{}, c.resumeStranded(ctx, a.InstanceID)
	}
	if err != nil {
		return struct{}{}, err
	}
	if err := NewRuntime(graph, c.db, c.dispatcher).ExecuteExecution(ctx, a.ExecutionID); err != nil {
		return struct{}{}, fmt.Errorf("%w: %w", ErrExecutionFailed, err)
	}
	return struct{}{}, nil
}

// ── Jobs ────────────────────────────────────────────────────────────────

type CompleteJobArgs struct {
	JobID     uuid.UUID              `json:"jobId"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

type CompleteJobOutcome struct {
	ProcessInstanceID uuid.UUID `json:"processInstanceId"`
	ProcessCompleted  bool      `json:"processCompleted"`
}

// CompleteJob completes a locked job and continues its process.
func CompleteJob(ctx context.Context, db *sqlx.DB, jobID uuid.UUID, variables map[string]interface{}) (CompleteJobOutcome, error) {
	var out CompleteJobOutcome
	err := cmdbus.Do(ctx, instanceOf(db, "jobs", jobID, service.ErrJobNotFound), CmdCompleteJob,
		CompleteJobArgs{JobID: jobID, Variables: variables}, &out)
	return out, err
}

func (c *commands) completeJob(ctx context.Context, a CompleteJobArgs) (CompleteJobOutcome, error) {
	result, err := service.CompleteJob(c.db, a.JobID, a.Variables)
	if errors.Is(err, service.ErrJobInvalidState) && cmdbus.Redelivered(ctx) {
		return CompleteJobOutcome{}, c.resumeJobInstance(ctx, a.JobID)
	}
	if err != nil {
		return CompleteJobOutcome{}, err
	}
	out := CompleteJobOutcome{ProcessInstanceID: result.ProcessInstanceID, ProcessCompleted: result.ProcessCompleted}
	if result.ProcessCompleted {
		return out, nil
	}
	if err := NewRuntime(result.Graph, c.db, c.dispatcher).ExecuteExecution(ctx, result.ExecutionID); err != nil {
		return out, fmt.Errorf("%w: %w", ErrExecutionFailed, err)
	}
	return out, nil
}

func (c *commands) resumeJobInstance(ctx context.Context, jobID uuid.UUID) error {
	var instanceID uuid.UUID
	if err := c.db.Get(&instanceID, `SELECT process_instance_id FROM public.jobs WHERE id = $1`, jobID); err != nil {
		return nil // archived with its instance: nothing left to resume
	}
	return c.resumeStranded(ctx, instanceID)
}

type FailJobArgs struct {
	JobID            uuid.UUID `json:"jobId"`
	RetriesRemaining int       `json:"retriesRemaining"`
	ErrorMessage     string    `json:"errorMessage,omitempty"`
	ErrorCode        string    `json:"errorCode,omitempty"`
	// Variables set on the instance before the failure (Camunda 8's
	// ThrowError carries some).
	Variables map[string]interface{} `json:"variables,omitempty"`
	Source    string                 `json:"source,omitempty"`
}

type FailJobOutcome struct {
	PermanentlyFailed  bool `json:"permanentlyFailed"`
	BoundaryTriggered  bool `json:"boundaryTriggered"`
	PropagatedToCaller bool `json:"propagatedToCaller"`
	// ContinueError is set when the failure was recorded but running the
	// process on from the catching event failed.
	ContinueError string `json:"continueError,omitempty"`
}

// FailJob records a job failure: retry, incident, or a BPMN error caught
// by a boundary event, an event sub-process or a calling process.
func FailJob(ctx context.Context, db *sqlx.DB, args FailJobArgs) (FailJobOutcome, error) {
	var out FailJobOutcome
	err := cmdbus.Do(ctx, instanceOf(db, "jobs", args.JobID, service.ErrJobNotFound), CmdFailJob, args, &out)
	return out, err
}

func (c *commands) failJob(ctx context.Context, a FailJobArgs) (FailJobOutcome, error) {
	var out FailJobOutcome
	if cmdbus.Redelivered(ctx) {
		var status string
		if err := c.db.Get(&status, `SELECT status FROM public.jobs WHERE id = $1`, a.JobID); err == nil && status != "locked" {
			return out, c.resumeJobInstance(ctx, a.JobID)
		}
	}
	if len(a.Variables) > 0 {
		var instanceID uuid.UUID
		if err := c.db.Get(&instanceID, `SELECT process_instance_id FROM public.jobs WHERE id = $1`, a.JobID); err != nil {
			return out, fmt.Errorf("%w: %s", service.ErrJobNotFound, a.JobID)
		}
		if err := c.mergeVariables(instanceID, uuid.Nil, false, a.Variables, a.Source); err != nil {
			return out, err
		}
	}
	result, err := service.FailJob(c.db, a.JobID, a.RetriesRemaining, a.ErrorMessage, a.ErrorCode)
	if err != nil {
		return out, err
	}
	out.PermanentlyFailed = result.PermanentlyFailed
	switch {
	case result.BoundaryTriggered:
		out.BoundaryTriggered = true
		if err := NewRuntime(result.Graph, c.db, c.dispatcher).ExecuteExecution(ctx, result.ExecutionID); err != nil {
			out.ContinueError = err.Error()
		}
	case result.PropagateError:
		out.PropagatedToCaller = true
		if _, err := NewRuntime(nil, c.db, c.dispatcher).PropagateErrorToCaller(ctx, result.ProcessInstanceID, a.ErrorCode); err != nil {
			out.ContinueError = err.Error()
		}
	}
	return out, nil
}

type SetJobRetriesArgs struct {
	JobID   uuid.UUID `json:"jobId"`
	Retries int       `json:"retries"`
}

// SetJobRetries gives a job new retries and resolves its incident.
func SetJobRetries(ctx context.Context, db *sqlx.DB, jobID uuid.UUID, retries int) error {
	return cmdbus.Do(ctx, instanceOf(db, "jobs", jobID, service.ErrJobNotFound), CmdSetJobRetries,
		SetJobRetriesArgs{JobID: jobID, Retries: retries}, nil)
}

func (c *commands) setJobRetries(ctx context.Context, a SetJobRetriesArgs) (struct{}, error) {
	return struct{}{}, service.SetJobRetries(c.db, a.JobID, a.Retries)
}

// ── Incidents ───────────────────────────────────────────────────────────

// ErrIncidentNotFound: no open incident with that id.
var ErrIncidentNotFound = errors.New("incident not found")

type ResolveIncidentArgs struct {
	IncidentID uuid.UUID `json:"incidentId"`
}

// ResolveIncidentOutcome.ResumeError is set when the incident was resolved
// but running its element again failed.
type ResolveIncidentOutcome struct {
	ResumeError string `json:"resumeError,omitempty"`
}

// ResolveIncident resolves an incident; one the runtime raised on an
// element runs that element again.
func ResolveIncident(ctx context.Context, db *sqlx.DB, incidentID uuid.UUID) (ResolveIncidentOutcome, error) {
	var out ResolveIncidentOutcome
	err := cmdbus.Do(ctx, instanceOf(db, "incidents", incidentID, ErrIncidentNotFound), CmdResolveIncident,
		ResolveIncidentArgs{IncidentID: incidentID}, &out)
	return out, err
}

func (c *commands) resolveIncident(ctx context.Context, a ResolveIncidentArgs) (ResolveIncidentOutcome, error) {
	var out ResolveIncidentOutcome
	inc, err := repository.NewIncidentRepository(c.db).GetByID(a.IncidentID)
	if err != nil || inc == nil {
		return out, fmt.Errorf("%w: %s", ErrIncidentNotFound, a.IncidentID)
	}
	if err := service.ResolveIncident(c.db, a.IncidentID); err != nil {
		return out, err
	}
	if err := ResumeAfterIncident(c.db, c.dispatcher, inc.ProcessInstanceID, a.IncidentID); err != nil {
		out.ResumeError = err.Error()
	}
	return out, nil
}

// ── Cancel ──────────────────────────────────────────────────────────────

type TerminateArgs struct {
	InstanceID uuid.UUID `json:"instanceId"`
	Reason     string    `json:"reason"`
}

// Terminate cancels a running process instance.
func Terminate(ctx context.Context, instanceID uuid.UUID, reason string) error {
	return cmdbus.Do(ctx, cmdbus.Key(instanceID), CmdTerminate, TerminateArgs{InstanceID: instanceID, Reason: reason}, nil)
}

func (c *commands) terminate(ctx context.Context, a TerminateArgs) (struct{}, error) {
	err := service.TerminateProcessInstance(c.db, c.dispatcher, a.InstanceID, a.Reason)
	if errors.Is(err, service.ErrProcessInstanceNotFound) && cmdbus.Redelivered(ctx) {
		return struct{}{}, nil
	}
	return struct{}{}, err
}

// ── User tasks ──────────────────────────────────────────────────────────

// ErrUserTaskNotFound: no user task with that id.
var ErrUserTaskNotFound = errors.New("task not found")

type CompleteUserTaskArgs struct {
	TaskID    uuid.UUID              `json:"taskId"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// CompleteUserTask completes a user task and continues its process.
func CompleteUserTask(ctx context.Context, db *sqlx.DB, taskID uuid.UUID, variables map[string]interface{}) error {
	return cmdbus.Do(ctx, instanceOf(db, "tasks", taskID, ErrUserTaskNotFound), CmdCompleteUserTask,
		CompleteUserTaskArgs{TaskID: taskID, Variables: variables}, nil)
}

func (c *commands) completeUserTask(ctx context.Context, a CompleteUserTaskArgs) (struct{}, error) {
	if cmdbus.Redelivered(ctx) {
		var row struct {
			Status            string    `db:"status"`
			ProcessInstanceID uuid.UUID `db:"process_instance_id"`
		}
		if err := c.db.Get(&row, `SELECT status, process_instance_id FROM public.tasks WHERE id = $1`, a.TaskID); err == nil && row.Status == "completed" {
			return struct{}{}, c.resumeStranded(ctx, row.ProcessInstanceID)
		}
	}
	return struct{}{}, c.tasks.CompleteTask(ctx, a.TaskID, a.Variables)
}

// ── Variables ───────────────────────────────────────────────────────────

type SetVariablesArgs struct {
	InstanceID uuid.UUID              `json:"instanceId"`
	ScopeID    uuid.UUID              `json:"scopeId"`
	Local      bool                   `json:"local"`
	Variables  map[string]interface{} `json:"variables"`
	Source     string                 `json:"source"`
}

// SetVariables sets variables on a scope of a running instance (Camunda
// 8): local ones on that scope only, the others propagate outwards from it.
func SetVariables(ctx context.Context, args SetVariablesArgs) error {
	return cmdbus.Do(ctx, cmdbus.Key(args.InstanceID), CmdSetVariables, args, nil)
}

func (c *commands) setVariables(ctx context.Context, a SetVariablesArgs) (struct{}, error) {
	return struct{}{}, c.mergeVariables(a.InstanceID, a.ScopeID, a.Local, a.Variables, a.Source)
}

func (c *commands) mergeVariables(instanceID, scopeID uuid.UUID, local bool, vars map[string]interface{}, source string) error {
	tx, err := c.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if local && scopeID != uuid.Nil {
		err = repository.SetLocalVariablesTx(tx, instanceID, scopeID, vars)
	} else {
		err = repository.SetVariablesTx(tx, instanceID, scopeID, vars)
	}
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	service.LogAuditErr("variables merged", service.NewAuditService(c.db).LogVariablesMerged(instanceID, vars, source))
	return nil
}

// ── Modification ────────────────────────────────────────────────────────

type ModifyArgs struct {
	InstanceID uuid.UUID         `json:"instanceId"`
	Moves      []MoveInstruction `json:"moves"`
}

// Modify moves tokens of a running instance (see ModifyInstance) and
// returns the new executions.
func Modify(ctx context.Context, instanceID uuid.UUID, moves []MoveInstruction) ([]uuid.UUID, error) {
	var out []uuid.UUID
	err := cmdbus.Do(ctx, cmdbus.Key(instanceID), CmdModify, ModifyArgs{InstanceID: instanceID, Moves: moves}, &out)
	return out, err
}

func (c *commands) modify(ctx context.Context, a ModifyArgs) ([]uuid.UUID, error) {
	graph, err := repository.NewEngineRepository(c.db, c.dispatcher).GetProcessGraphByInstanceID(a.InstanceID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", service.ErrProcessInstanceNotFound, a.InstanceID)
	}
	return NewRuntime(graph, c.db, c.dispatcher).ModifyInstance(ctx, a.InstanceID, a.Moves)
}

// ── Messages and signals ────────────────────────────────────────────────

// Publish publishes a Camunda 8 message. Like Zeebe, a message goes to the
// partition of its correlation key, so messages with the same key are
// handled one after the other.
func Publish(ctx context.Context, msg PublishedMessage) (PublishResult, error) {
	var out PublishResult
	key := cmdbus.NameKey("message", msg.TenantID, msg.Name, msg.CorrelationKey)
	err := cmdbus.Do(ctx, cmdbus.Key(key), CmdPublishMessage, msg, &out)
	return out, err
}

func (c *commands) publishMessage(ctx context.Context, msg PublishedMessage) (PublishResult, error) {
	return PublishMessage(ctx, c.db, c.dispatcher, msg)
}

type BroadcastSignalArgs struct {
	Name      string                 `json:"name"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

type BroadcastSignalOutcome struct {
	Resumed int      `json:"resumed"`
	Errors  []string `json:"errors"`
}

// Signal broadcasts a signal to every instance waiting for it and starts
// the processes it starts.
func Signal(ctx context.Context, name string, variables map[string]interface{}) (int, []string, error) {
	var out BroadcastSignalOutcome
	err := cmdbus.Do(ctx, cmdbus.Key(cmdbus.NameKey("signal", name)), CmdBroadcastSignal,
		BroadcastSignalArgs{Name: name, Variables: variables}, &out)
	return out.Resumed, out.Errors, err
}

func (c *commands) broadcastSignal(ctx context.Context, a BroadcastSignalArgs) (BroadcastSignalOutcome, error) {
	n, errs, err := BroadcastSignal(c.db, c.dispatcher, a.Name, a.Variables)
	return BroadcastSignalOutcome{Resumed: n, Errors: errs}, err
}

// ── Timers and conditions ───────────────────────────────────────────────

// PostStartTimer fires a timer start event: a new instance, started on the
// partition of the timer.
func PostStartTimer(ctx context.Context, job models.TimerJob) error {
	return cmdbus.Post(ctx, cmdbus.Key(job.ID), CmdFireStartTimer, job)
}

func (c *commands) fireStartTimer(ctx context.Context, job models.TimerJob) (struct{}, error) {
	return struct{}{}, FireStartTimer(ctx, c.db, c.dispatcher, job)
}

type checkConditionsArgs struct {
	InstanceID uuid.UUID `json:"instanceId"`
}

func (c *commands) checkConditions(ctx context.Context, a checkConditionsArgs) (struct{}, error) {
	return struct{}{}, checkConditions(c.db, c.dispatcher, []uuid.UUID{a.InstanceID})
}

// CmdDeliverKeptMessage delivers a kept (time-to-live) message to the
// subscriptions opened, or processes deployed, since it was published.
const CmdDeliverKeptMessage = "DeliverKeptMessage"

func (c *commands) deliverKeptMessage(ctx context.Context, d keptDelivery) (struct{}, error) {
	return struct{}{}, runKeptDelivery(ctx, c.db, c.dispatcher, d)
}
