package zeebegrpc

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/partition"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/internal/zeebegrpc/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The remaining Gateway RPCs of the zeebe-node client's protocol, all built
// on the same service layer as the RPCs in server.go.

// ── ThrowError ──────────────────────────────────────────────────────────

// ThrowError reports a BPMN error for a job: the nearest error catch event
// matching errorCode handles it (error boundary or error event
// sub-process, see engine.ProcessGraph.FindErrorCatcher); with no catcher
// an incident is raised. It's a job failure with no retries left and an
// error code.
func (s *Server) ThrowError(ctx context.Context, req *pb.ThrowErrorRequest) (*pb.ThrowErrorResponse, error) {
	if req.ErrorCode == "" {
		return nil, status.Error(codes.InvalidArgument, "errorCode is required")
	}
	jobID, err := s.zeebeKeys.ResolveKey("job", req.JobKey)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job key %d not found", req.JobKey)
	}
	vars, err := parseVariables(req.Variables)
	if err != nil {
		return nil, err
	}

	instanceID, writePath, err := partition.LookupJobOwner(s.db, jobID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job key %d not found", req.JobKey)
	}
	if writePath == "partitioned" {
		if len(vars) > 0 {
			return nil, status.Error(codes.FailedPrecondition, "variables on ThrowError aren't supported for partitioned-write-path instances")
		}
		if err := partition.FailJob(instanceID, jobID, 0, req.ErrorMessage, req.ErrorCode); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		return &pb.ThrowErrorResponse{}, nil
	}

	if len(vars) > 0 {
		if err := s.setVariables(instanceID, uuid.Nil, false, vars, "throwError"); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	result, err := service.FailJob(s.db, jobID, 0, req.ErrorMessage, req.ErrorCode)
	if err != nil {
		return nil, mapJobError(err)
	}
	if result.BoundaryTriggered {
		rt := runtime.NewRuntime(result.Graph, s.db, s.dispatcher)
		if err := rt.ExecuteExecution(ctx, result.ExecutionID); err != nil {
			return nil, status.Errorf(codes.Internal, "error caught, but continuing the process failed: %v", err)
		}
	}
	if result.PropagateError {
		rt := runtime.NewRuntime(nil, s.db, s.dispatcher)
		if _, err := rt.PropagateErrorToCaller(ctx, result.ProcessInstanceID, req.ErrorCode); err != nil {
			return nil, status.Errorf(codes.Internal, "error caught by the calling process, but continuing it failed: %v", err)
		}
	}
	return &pb.ThrowErrorResponse{}, nil
}

// ── SetVariables ────────────────────────────────────────────────────────

// SetVariables sets variables on a scope of a running process instance,
// as in Camunda 8: elementInstanceKey is a process instance key or an
// element instance key (an activated job's elementInstanceKey, i.e. its
// activity's scope). With local, the variables are set on that scope
// only; otherwise each one propagates outwards from it (see
// repository.SetVariablesTx).
func (s *Server) SetVariables(ctx context.Context, req *pb.SetVariablesRequest) (*pb.SetVariablesResponse, error) {
	scopeID := uuid.Nil
	instanceID, err := s.zeebeKeys.ResolveKey("process_instance", req.ElementInstanceKey)
	if err != nil {
		execID, execErr := s.zeebeKeys.ResolveKey("element_instance", req.ElementInstanceKey)
		if execErr != nil {
			return nil, status.Errorf(codes.NotFound, "element instance key %d not found", req.ElementInstanceKey)
		}
		exec, execErr := s.engineRepo.GetExecutionByID(execID)
		if execErr != nil || exec == nil {
			return nil, status.Errorf(codes.NotFound, "element instance key %d not found", req.ElementInstanceKey)
		}
		instanceID, scopeID = exec.ProcessInstanceID, execID
	}
	vars, err := parseVariables(req.Variables)
	if err != nil {
		return nil, err
	}
	if len(vars) == 0 {
		return nil, status.Error(codes.InvalidArgument, "variables must be a non-empty JSON object")
	}

	inst, err := s.processInstanceRepo.FindByID(instanceID)
	if err != nil || inst == nil || inst.Status != "running" {
		return nil, status.Errorf(codes.NotFound, "no running process instance with key %d", req.ElementInstanceKey)
	}
	if allowed, permErr := service.CanAccessProcessInstance(s.db, UserIDFromContext(ctx), instanceID, "START"); permErr == nil && !allowed {
		return nil, status.Errorf(codes.NotFound, "no running process instance with key %d", req.ElementInstanceKey)
	}
	if def, err := s.processRepo.FindProcessDefinitionByID(inst.ProcessDefinitionID); err == nil && def.WritePath == "partitioned" {
		return nil, status.Error(codes.FailedPrecondition, "SetVariables isn't supported for partitioned-write-path instances")
	}

	if err := s.setVariables(instanceID, scopeID, req.Local, vars, "setVariables"); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	key, err := s.zeebeKeys.GetOrAssignKey("set_variables", uuid.New())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.SetVariablesResponse{Key: key}, nil
}

// setVariables sets vars on scopeID (uuid.Nil: the instance), locally or
// propagating outwards.
func (s *Server) setVariables(instanceID, scopeID uuid.UUID, local bool, vars map[string]interface{}, source string) error {
	tx, err := s.db.Beginx()
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
	service.LogAuditErr("variables merged", service.NewAuditService(s.db).LogVariablesMerged(instanceID, vars, source))
	return nil
}

// ── CreateProcessInstanceWithResult ─────────────────────────────────────

const defaultWithResultTimeout = 15 * time.Second

// CreateProcessInstanceWithResult starts an instance like
// CreateProcessInstance, then waits for it to complete and returns its
// variables (all of them, or only fetchVariables).
func (s *Server) CreateProcessInstanceWithResult(ctx context.Context, req *pb.CreateProcessInstanceWithResultRequest) (*pb.CreateProcessInstanceWithResultResponse, error) {
	if req.Request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	created, err := s.CreateProcessInstance(ctx, req.Request)
	if err != nil {
		return nil, err
	}
	instanceID, err := s.zeebeKeys.ResolveKey("process_instance", created.ProcessInstanceKey)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	timeout := defaultWithResultTimeout
	if req.RequestTimeout > 0 {
		timeout = time.Duration(req.RequestTimeout) * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for {
		state, err := s.instanceState(instanceID)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		switch state {
		case "completed":
			vars, err := s.finishedInstanceVariables(instanceID)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			body, err := json.Marshal(pick(vars, req.FetchVariables))
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			return &pb.CreateProcessInstanceWithResultResponse{
				ProcessDefinitionKey: created.ProcessDefinitionKey,
				BpmnProcessId:        created.BpmnProcessId,
				Version:              created.Version,
				ProcessInstanceKey:   created.ProcessInstanceKey,
				Variables:            string(body),
				TenantId:             created.TenantId,
			}, nil
		case "terminated":
			return nil, status.Errorf(codes.Aborted, "process instance %d was terminated before completing", created.ProcessInstanceKey)
		}
		if time.Now().After(deadline) {
			return nil, status.Errorf(codes.DeadlineExceeded, "process instance %d did not complete within %s", created.ProcessInstanceKey, timeout)
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// instanceState returns "running", "completed" or "terminated", looking in
// history once a finished instance has been archived out of the live tables.
func (s *Server) instanceState(instanceID uuid.UUID) (string, error) {
	var st string
	err := s.db.Get(&st, `SELECT status::text FROM public.process_instances WHERE id = $1`, instanceID)
	if err == nil {
		return st, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	err = s.db.Get(&st, `SELECT state FROM public.historic_process_instances WHERE id = $1`, instanceID)
	if errors.Is(err, sql.ErrNoRows) {
		return "running", nil // between the live delete and the history read: try again
	}
	return st, err
}

func (s *Server) finishedInstanceVariables(instanceID uuid.UUID) (map[string]interface{}, error) {
	if vars, err := s.processInstanceRepo.GetVariables(instanceID); err == nil && len(vars) > 0 {
		return vars, nil
	}
	return repository.NewHistoricArchiveRepository(s.db).FindVariablesByProcessInstance(instanceID)
}

func pick(vars map[string]interface{}, names []string) map[string]interface{} {
	if len(names) == 0 {
		return vars
	}
	out := make(map[string]interface{}, len(names))
	for _, n := range names {
		if v, ok := vars[n]; ok {
			out[n] = v
		}
	}
	return out
}

// ── StreamActivatedJobs (server-streaming) ──────────────────────────────

// StreamActivatedJobs pushes jobs of a type to the worker as they become
// available, until the client closes the stream: the long-lived
// counterpart of ActivateJobs' one-batch long poll.
func (s *Server) StreamActivatedJobs(req *pb.StreamActivatedJobsRequest, stream pb.Gateway_StreamActivatedJobsServer) error {
	if req.Type == "" {
		return status.Error(codes.InvalidArgument, "type is required")
	}
	lockDuration := int(req.Timeout)
	if lockDuration <= 0 {
		lockDuration = 30000
	}
	ctx := stream.Context()
	for {
		if ctx.Err() != nil {
			return nil
		}
		// Snapshot the wake channel before fetching (see ActivateJobs).
		wake := repository.JobWakeChannel(req.Type)
		jobs, err := s.fetchAndLockOneBatch(req.Type, req.Worker, lockDuration, 32)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		for _, job := range jobs {
			if err := stream.Send(job); err != nil {
				return err
			}
		}
		if len(jobs) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-time.After(activateJobsFallbackPoll):
		}
	}
}

// ── DeployProcess (deprecated, replaced by DeployResource) ──────────────

func (s *Server) DeployProcess(ctx context.Context, req *pb.DeployProcessRequest) (*pb.DeployProcessResponse, error) {
	resources := make([]*pb.Resource, len(req.Processes))
	for i, p := range req.Processes {
		resources[i] = &pb.Resource{Name: p.Name, Content: p.Definition}
	}
	deployed, err := s.DeployResource(ctx, &pb.DeployResourceRequest{Resources: resources})
	if err != nil {
		return nil, err
	}
	resp := &pb.DeployProcessResponse{Key: deployed.Key}
	for _, d := range deployed.Deployments {
		if p := d.GetProcess(); p != nil {
			resp.Processes = append(resp.Processes, p)
		}
	}
	return resp, nil
}

// ── DeleteResource ──────────────────────────────────────────────────────

// DeleteResource deletes a process definition by its key. Like Zeebe, it
// refuses while instances of that definition still exist; finished
// instances stay in history.
func (s *Server) DeleteResource(ctx context.Context, req *pb.DeleteResourceRequest) (*pb.DeleteResourceResponse, error) {
	defID, err := s.zeebeKeys.ResolveKey("process_definition", req.ResourceKey)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "resource key %d not found", req.ResourceKey)
	}
	def, err := s.processRepo.FindProcessDefinitionByID(defID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "resource key %d not found", req.ResourceKey)
	}
	if allowed, permErr := service.CanAccessProcess(s.db, UserIDFromContext(ctx), def.ProcessKey, "MANAGE"); permErr == nil && !allowed {
		return nil, status.Error(codes.PermissionDenied, "you do not have MANAGE access to this process")
	}

	var live int
	if err := s.db.Get(&live, `SELECT count(*) FROM public.process_instances WHERE process_definition_id = $1`, defID); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if live > 0 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"process definition %s version %d still has %d process instance(s); cancel or finish them first", def.ProcessKey, def.Version, live)
	}
	if _, err := s.processRepo.DeleteProcessDefinition(defID); err != nil {
		if strings.Contains(err.Error(), "foreign key") {
			return nil, status.Errorf(codes.FailedPrecondition, "process definition is still referenced: %v", err)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	service.RearmStartEvents(s.db, def.ProcessKey)
	return &pb.DeleteResourceResponse{}, nil
}

// parseVariables decodes a Zeebe variables document (a JSON object, or
// empty).
func parseVariables(doc string) (map[string]interface{}, error) {
	if strings.TrimSpace(doc) == "" {
		return nil, nil
	}
	var vars map[string]interface{}
	if err := json.Unmarshal([]byte(doc), &vars); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "variables must be a JSON object: %v", err)
	}
	return vars, nil
}
