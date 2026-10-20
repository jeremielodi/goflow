// Package zeebegrpc implements a subset of Zeebe's Gateway gRPC protocol
// (see internal/zeebegrpc/proto/gateway.proto for scope and rationale) on
// top of the same engine/service/repository layers the existing
// /engine-rest and /v2 REST APIs use — this package only translates
// request/response shapes and resolves UUID<->int64 keys, it does not
// duplicate engine logic.
package zeebegrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jeremielodi/goflow/internal/dmn"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
	"github.com/jeremielodi/goflow/internal/partition"
	"github.com/jeremielodi/goflow/internal/repository"
	"github.com/jeremielodi/goflow/internal/runtime"
	"github.com/jeremielodi/goflow/internal/service"
	"github.com/jeremielodi/goflow/internal/zeebegrpc/pb"
	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedGatewayServer

	db                  *sqlx.DB
	dispatcher          *events.TaskEventDispatcher
	processRepo         *repository.ProcessRepository
	processInstanceRepo *repository.ProcessInstanceRepository
	externalTaskRepo    *repository.ExternalTaskRepository
	engineRepo          *repository.EngineRepository
	incidentRepo        *repository.IncidentRepository
	zeebeKeys           *repository.ZeebeKeyRepository
}

func NewServer(db *sqlx.DB, dispatcher *events.TaskEventDispatcher) *Server {
	return &Server{
		db:                  db,
		dispatcher:          dispatcher,
		processRepo:         repository.NewProcessRepository(db),
		processInstanceRepo: repository.NewProcessInstanceRepository(db),
		externalTaskRepo:    repository.NewExternalTaskRepository(db),
		engineRepo:          repository.NewEngineRepository(db, dispatcher),
		incidentRepo:        repository.NewIncidentRepository(db),
		zeebeKeys:           repository.NewZeebeKeyRepository(db),
	}
}

// ── Topology ────────────────────────────────────────────────────────────

func (s *Server) Topology(ctx context.Context, req *pb.TopologyRequest) (*pb.TopologyResponse, error) {
	topo, err := partition.Describe(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	resp := &pb.TopologyResponse{
		ClusterSize:       int32(len(topo.Brokers)),
		PartitionsCount:   int32(topo.PartitionsCount),
		ReplicationFactor: int32(topo.ReplicationFactor),
		GatewayVersion:    "goflow-dev",
	}
	for i, b := range topo.Brokers {
		info := &pb.BrokerInfo{NodeId: int32(i), Host: b.Host, Port: 26500, Version: "goflow-dev"}
		for _, p := range b.Partitions {
			info.Partitions = append(info.Partitions, &pb.Partition{
				PartitionId: int32(p),
				Role:        pb.PartitionBrokerRole_LEADER,
				Health:      pb.PartitionBrokerHealth_HEALTHY,
			})
		}
		resp.Brokers = append(resp.Brokers, info)
	}
	return resp, nil
}

// ── DeployResource ──────────────────────────────────────────────────────

func (s *Server) DeployResource(ctx context.Context, req *pb.DeployResourceRequest) (*pb.DeployResourceResponse, error) {
	userID := UserIDFromContext(ctx)

	resources := make([]service.RawResource, len(req.Resources))
	for i, r := range req.Resources {
		resources[i] = service.RawResource{Filename: r.Name, Content: r.Content}
	}

	// Real Zeebe clients don't send a deployment name (deployments aren't
	// a user-facing concept in that protocol) — synthesize a unique one.
	deploymentName := fmt.Sprintf("grpc-deployment-%d", time.Now().UnixNano())

	result, err := service.DeployResources(s.db, deploymentName, req.TenantId, resources, userID)
	if err != nil {
		if errors.Is(err, service.ErrDeployPermissionDenied) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	deployKey, err := s.zeebeKeys.GetOrAssignKey("deployment", result.DeploymentID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	deployments := make([]*pb.Deployment, 0, len(result.ProcessDefinitions))
	for _, d := range result.ProcessDefinitions {
		defKey, err := s.zeebeKeys.GetOrAssignKey("process_definition", d.ID)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		deployments = append(deployments, &pb.Deployment{
			Metadata: &pb.Deployment_Process{
				Process: &pb.ProcessMetadata{
					BpmnProcessId:        d.Key,
					Version:              int32(d.Version),
					ProcessDefinitionKey: defKey,
					ResourceName:         d.ResourceName,
					TenantId:             req.TenantId,
				},
			},
		})
	}

	return &pb.DeployResourceResponse{
		Key:         deployKey,
		Deployments: deployments,
		TenantId:    req.TenantId,
	}, nil
}

// ── CreateProcessInstance ───────────────────────────────────────────────

func (s *Server) resolveDefinition(req *pb.CreateProcessInstanceRequest) (models.ProcessDefinition, error) {
	if req.ProcessDefinitionKey != 0 {
		id, err := s.zeebeKeys.ResolveKey("process_definition", req.ProcessDefinitionKey)
		if err != nil {
			return models.ProcessDefinition{}, fmt.Errorf("process definition key %d not found", req.ProcessDefinitionKey)
		}
		return s.processRepo.FindProcessDefinitionByID(id)
	}
	if req.BpmnProcessId == "" {
		return models.ProcessDefinition{}, fmt.Errorf("either processDefinitionKey or bpmnProcessId must be set")
	}
	if req.Version <= 0 {
		return s.processRepo.FindLatestProcessDefinitionByKey(req.BpmnProcessId)
	}
	return s.processRepo.FindProcessDefinitionByKeyAndVersion(req.BpmnProcessId, int(req.Version))
}

func (s *Server) CreateProcessInstance(ctx context.Context, req *pb.CreateProcessInstanceRequest) (*pb.CreateProcessInstanceResponse, error) {
	userID := UserIDFromContext(ctx)

	def, err := s.resolveDefinition(req)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	if allowed, permErr := service.CanAccessProcess(s.db, userID, def.ProcessKey, "START"); permErr == nil && !allowed {
		return nil, status.Error(codes.PermissionDenied, "you do not have START access to this process")
	}

	var vars map[string]interface{}
	if req.Variables != "" {
		if err := json.Unmarshal([]byte(req.Variables), &vars); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid variables JSON: %v", err)
		}
	}

	tenantID := req.TenantId
	instanceID, _, err := runtime.StartInstance(ctx, def.ID, &tenantID, vars)
	if errors.Is(err, runtime.ErrExecutionFailed) {
		return nil, status.Errorf(codes.Internal, "failed to execute process: %v", err)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	defKey, err := s.zeebeKeys.GetOrAssignKey("process_definition", def.ID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	instKey, err := s.zeebeKeys.GetOrAssignKey("process_instance", instanceID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.CreateProcessInstanceResponse{
		ProcessDefinitionKey: defKey,
		BpmnProcessId:        def.ProcessKey,
		Version:              int32(def.Version),
		ProcessInstanceKey:   instKey,
		TenantId:             req.TenantId,
	}, nil
}

// ── ActivateJobs (server-streaming) ─────────────────────────────────────

const (
	defaultActivateJobsTimeout = 10 * time.Second
	// activateJobsFallbackPoll is a safety-net interval, not the primary
	// wake-up mechanism: repository.NotifyJobAvailable wakes this loop
	// near-instantly when a new job of this type is created, but a job
	// whose lock simply expired (locked_until < NOW()) becomes fetchable
	// again without any notify firing, so a fallback poll is still needed
	// to pick those up.
	activateJobsFallbackPoll = 2 * time.Second
)

func (s *Server) ActivateJobs(req *pb.ActivateJobsRequest, stream pb.Gateway_ActivateJobsServer) error {
	requestTimeout := defaultActivateJobsTimeout
	if req.RequestTimeout > 0 {
		requestTimeout = time.Duration(req.RequestTimeout) * time.Millisecond
	}
	lockDuration := int(req.Timeout / int64(time.Millisecond))
	if lockDuration <= 0 {
		lockDuration = 30000
	}
	maxJobs := int(req.MaxJobsToActivate)
	if maxJobs <= 0 {
		maxJobs = 1
	}

	deadline := time.Now().Add(requestTimeout)
	ctx := stream.Context()

	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return nil
		}

		// Snapshot the wake channel *before* fetching, so a job created
		// in the gap between this fetch and the select below still wakes
		// us — NotifyJobAvailable installs a fresh channel every time it
		// fires, so this snapshot can only be closed by a notify that
		// happens after this point.
		wake := repository.JobWakeChannel(req.Type)

		jobs, err := s.fetchAndLockOneBatch(req.Type, req.Worker, lockDuration, maxJobs)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}

		if len(jobs) > 0 {
			return stream.Send(&pb.ActivateJobsResponse{Jobs: jobs})
		}

		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-time.After(activateJobsFallbackPoll):
		}
	}

	return nil
}

func (s *Server) fetchAndLockOneBatch(jobType, worker string, lockDuration, maxJobs int) ([]*pb.ActivatedJob, error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	topicJobs, err := s.externalTaskRepo.FetchAndLockJobsTx(tx, []repository.TopicConfig{
		{TopicName: jobType, LockDuration: lockDuration},
	}, worker, maxJobs)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	var activated []*pb.ActivatedJob
	for _, tj := range topicJobs {
		for _, job := range tj.Jobs {
			pbJob, err := s.toActivatedJob(job, tj.TopicName, worker)
			if err != nil {
				continue
			}
			activated = append(activated, pbJob)
		}
	}
	return activated, nil
}

func (s *Server) toActivatedJob(job repository.LockedJob, jobType, worker string) (*pb.ActivatedJob, error) {
	jobKey, err := s.zeebeKeys.GetOrAssignKey("job", job.ID)
	if err != nil {
		return nil, err
	}

	inst, err := s.processInstanceRepo.FindByID(job.ProcessInstanceID)
	if err != nil || inst == nil {
		return nil, fmt.Errorf("process instance not found for job %s", job.ID)
	}
	instKey, err := s.zeebeKeys.GetOrAssignKey("process_instance", job.ProcessInstanceID)
	if err != nil {
		return nil, err
	}
	defKey, err := s.zeebeKeys.GetOrAssignKey("process_definition", inst.ProcessDefinitionID)
	if err != nil {
		return nil, err
	}
	// The activity's own key, so SetVariables can target its scope.
	elementKey, err := s.zeebeKeys.GetOrAssignKey("element_instance", job.ExecutionID)
	if err != nil {
		return nil, err
	}

	customHeaders := "{}"
	if graph, err := s.engineRepo.GetProcessGraphByInstanceID(job.ProcessInstanceID); err == nil && graph != nil {
		if n, ok := graph.Nodes[job.CurrentElementID]; ok && len(n.TaskHeaders) > 0 {
			if b, err := json.Marshal(n.TaskHeaders); err == nil {
				customHeaders = string(b)
			}
		}
	}

	variablesJSON := "{}"
	if len(job.Payload) > 0 {
		variablesJSON = string(job.Payload)
	}

	return &pb.ActivatedJob{
		Key:                      jobKey,
		Type:                     jobType,
		ProcessInstanceKey:       instKey,
		BpmnProcessId:            inst.ProcessKey,
		ProcessDefinitionVersion: int32(inst.Version),
		ProcessDefinitionKey:     defKey,
		ElementId:                job.CurrentElementID,
		ElementInstanceKey:       elementKey,
		CustomHeaders:            customHeaders,
		Worker:                   worker,
		Retries:                  int32(job.Retries),
		Deadline:                 time.Now().Add(30 * time.Second).UnixMilli(),
		Variables:                variablesJSON,
		TenantId:                 "",
	}, nil
}

// ── CompleteJob ─────────────────────────────────────────────────────────

func (s *Server) CompleteJob(ctx context.Context, req *pb.CompleteJobRequest) (*pb.CompleteJobResponse, error) {
	jobID, err := s.zeebeKeys.ResolveKey("job", req.JobKey)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job key %d not found", req.JobKey)
	}

	var vars map[string]interface{}
	if req.Variables != "" {
		if err := json.Unmarshal([]byte(req.Variables), &vars); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid variables JSON: %v", err)
		}
	}

	if _, err := runtime.CompleteJob(ctx, s.db, jobID, vars); err != nil {
		if errors.Is(err, runtime.ErrExecutionFailed) {
			return nil, status.Errorf(codes.Internal, "failed to resume process execution: %v", err)
		}
		return nil, mapJobError(err)
	}

	return &pb.CompleteJobResponse{}, nil
}

// ── FailJob ─────────────────────────────────────────────────────────────

func (s *Server) FailJob(ctx context.Context, req *pb.FailJobRequest) (*pb.FailJobResponse, error) {
	jobID, err := s.zeebeKeys.ResolveKey("job", req.JobKey)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job key %d not found", req.JobKey)
	}

	// req.Retries is already "count to leave remaining after this
	// failure" (Zeebe's own convention) — no extra decrement, unlike the
	// REST HandleFailure path which uses Camunda 7's convention.

	// Continuing from a boundary event may fail without failing the call:
	// the failure itself is recorded.
	if _, err := runtime.FailJob(ctx, s.db, runtime.FailJobArgs{
		JobID:            jobID,
		RetriesRemaining: int(req.Retries),
		ErrorMessage:     req.ErrorMessage,
	}); err != nil {
		return nil, mapJobError(err)
	}

	return &pb.FailJobResponse{}, nil
}

func mapJobError(err error) error {
	switch {
	case errors.Is(err, service.ErrJobNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, service.ErrJobInvalidState):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// ── PublishMessage ──────────────────────────────────────────────────────

func (s *Server) PublishMessage(ctx context.Context, req *pb.PublishMessageRequest) (*pb.PublishMessageResponse, error) {
	var vars map[string]interface{}
	if req.Variables != "" {
		if err := json.Unmarshal([]byte(req.Variables), &vars); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid variables JSON: %v", err)
		}
	}

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	res, err := runtime.Publish(ctx, runtime.PublishedMessage{
		Name:           req.Name,
		CorrelationKey: req.CorrelationKey,
		Variables:      vars,
		TimeToLive:     time.Duration(req.TimeToLive) * time.Millisecond,
		MessageID:      req.MessageId,
		TenantID:       req.TenantId,
	})
	if err != nil {
		if errors.Is(err, runtime.ErrMessageAlreadyPublished) {
			return nil, status.Errorf(codes.AlreadyExists, "message %q with id %q is already published", req.Name, req.MessageId)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	key, err := s.zeebeKeys.GetOrAssignKey("message", res.MessageID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.PublishMessageResponse{Key: key, TenantId: req.TenantId}, nil
}

// ── CancelProcessInstance ───────────────────────────────────────────────

func (s *Server) CancelProcessInstance(ctx context.Context, req *pb.CancelProcessInstanceRequest) (*pb.CancelProcessInstanceResponse, error) {
	instanceID, err := s.zeebeKeys.ResolveKey("process_instance", req.ProcessInstanceKey)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "process instance key %d not found", req.ProcessInstanceKey)
	}

	reason := "cancelled via gRPC CancelProcessInstance"

	if err := runtime.Terminate(ctx, instanceID, reason); err != nil {
		if errors.Is(err, service.ErrProcessInstanceNotFound) {
			return nil, status.Errorf(codes.NotFound, "process instance key %d not found", req.ProcessInstanceKey)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.CancelProcessInstanceResponse{}, nil
}

// ── EvaluateDecision ────────────────────────────────────────────────────

func (s *Server) EvaluateDecision(ctx context.Context, req *pb.EvaluateDecisionRequest) (*pb.EvaluateDecisionResponse, error) {
	if req.DecisionId == "" {
		return nil, status.Error(codes.InvalidArgument, "decisionId is required (decisionKey lookup is not supported: GoFlow has not assigned int64 keys to decisions)")
	}

	dmnRepo := repository.NewDMNRepository(s.db)
	decision, err := dmnRepo.FindLatestDecisionByKey(req.DecisionId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "no decision found for id %s", req.DecisionId)
	}

	var parsed dmn.Decision
	if err := json.Unmarshal(decision.ParsedTable, &parsed); err != nil || parsed.DecisionTable == nil {
		return nil, status.Error(codes.Internal, "decision has no evaluable decision table")
	}

	var vars map[string]interface{}
	if req.Variables != "" {
		if err := json.Unmarshal([]byte(req.Variables), &vars); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid variables JSON: %v", err)
		}
	}

	result, err := dmn.EvaluateDecisionTable(parsed.DecisionTable, vars)
	if err != nil {
		return &pb.EvaluateDecisionResponse{
			FailedDecisionId: req.DecisionId,
			FailureMessage:   err.Error(),
		}, nil
	}

	outputJSON, err := json.Marshal(result)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.EvaluateDecisionResponse{
		DecisionId:      decision.DecisionKey,
		DecisionVersion: int32(decision.Version),
		DecisionOutput:  string(outputJSON),
		TenantId:        req.TenantId,
	}, nil
}

// ── ResolveIncident ─────────────────────────────────────────────────────

func (s *Server) ResolveIncident(ctx context.Context, req *pb.ResolveIncidentRequest) (*pb.ResolveIncidentResponse, error) {
	incidentID, err := s.zeebeKeys.ResolveKey("incident", req.IncidentKey)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "incident key %d not found", req.IncidentKey)
	}

	out, err := runtime.ResolveIncident(ctx, s.db, incidentID)
	if errors.Is(err, runtime.ErrIncidentNotFound) {
		return nil, status.Errorf(codes.NotFound, "incident key %d not found", req.IncidentKey)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if out.ResumeError != "" {
		return nil, status.Errorf(codes.Internal, "incident resolved, but running the element again failed: %s", out.ResumeError)
	}

	return &pb.ResolveIncidentResponse{}, nil
}

// ── ModifyProcessInstance ───────────────────────────────────────────────
// Only one implied move per call is supported: the target comes from
// activateInstructions[0].ElementId, and the source is derived
// automatically as the instance's single active execution rather than
// from terminateInstructions — see the file-level comment in gateway.proto
// for why (the real SDK's TerminateInstruction carries only an opaque
// elementInstanceKey GoFlow has no mapping for). Multi-token instances
// (parallel gateways, multi-instance) aren't resolvable this way and stay
// out of scope for gRPC modification; POST /v2/process-instances/:id/modification
// (explicit sourceElementId/targetElementId pairs) remains the way to
// handle those.
func (s *Server) ModifyProcessInstance(ctx context.Context, req *pb.ModifyProcessInstanceRequest) (*pb.ModifyProcessInstanceResponse, error) {
	if len(req.ActivateInstructions) != 1 {
		return nil, status.Error(codes.InvalidArgument, "exactly one activateInstruction is supported (single token move only)")
	}
	targetElementID := req.ActivateInstructions[0].ElementId
	if targetElementID == "" {
		return nil, status.Error(codes.InvalidArgument, "activateInstructions[0].elementId is required")
	}

	instanceID, err := s.zeebeKeys.ResolveKey("process_instance", req.ProcessInstanceKey)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "process instance key %d not found", req.ProcessInstanceKey)
	}

	inst, err := s.processInstanceRepo.FindByID(instanceID)
	if err != nil || inst == nil {
		return nil, status.Errorf(codes.NotFound, "process instance not found")
	}

	activeExecutions, err := s.processInstanceRepo.GetActiveExecutions(instanceID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if len(activeExecutions) != 1 {
		return nil, status.Errorf(codes.FailedPrecondition, "expected exactly one active execution to move, found %d (multi-token modification is not supported over gRPC — use POST /v2/process-instances/:id/modification instead)", len(activeExecutions))
	}

	moves := []runtime.MoveInstruction{{
		SourceElementID: activeExecutions[0].CurrentElementID,
		TargetElementID: targetElementID,
	}}

	if _, err := runtime.Modify(ctx, instanceID, moves); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &pb.ModifyProcessInstanceResponse{}, nil
}

// ── UpdateJobRetries ────────────────────────────────────────────────────
// Mirrors internal/api/incident_controller.go's SetJobRetries REST
// handler exactly: same function, same partition.LookupJobOwner gate.

func (s *Server) UpdateJobRetries(ctx context.Context, req *pb.UpdateJobRetriesRequest) (*pb.UpdateJobRetriesResponse, error) {
	if req.Retries <= 0 {
		return nil, status.Error(codes.InvalidArgument, "retries must be positive")
	}
	jobID, err := s.zeebeKeys.ResolveKey("job", req.JobKey)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job key %d not found", req.JobKey)
	}

	if err := runtime.SetJobRetries(ctx, s.db, jobID, int(req.Retries)); err != nil {
		return nil, mapJobError(err)
	}

	return &pb.UpdateJobRetriesResponse{}, nil
}

// ── BroadcastSignal ─────────────────────────────────────────────────────

func (s *Server) BroadcastSignal(ctx context.Context, req *pb.BroadcastSignalRequest) (*pb.BroadcastSignalResponse, error) {
	if req.SignalName == "" {
		return nil, status.Error(codes.InvalidArgument, "signalName is required")
	}
	var vars map[string]interface{}
	if req.Variables != "" {
		if err := json.Unmarshal([]byte(req.Variables), &vars); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid variables JSON: %v", err)
		}
	}

	if _, _, err := runtime.Signal(ctx, req.SignalName, vars); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Zeebe assigns every broadcast signal a key; GoFlow has no signal
	// entity to key off, so mint a fresh one via the same zeebe_keys table
	// PublishMessage already uses for the same reason — nothing ever needs
	// to resolve it back.
	key, err := s.zeebeKeys.GetOrAssignKey("signal", uuid.New())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.BroadcastSignalResponse{Key: key}, nil
}
