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
	"os"
	"time"

	"github.com/jeremielodi/goflow/internal/engine"
	"github.com/jeremielodi/goflow/internal/events"
	"github.com/jeremielodi/goflow/internal/models"
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
		zeebeKeys:           repository.NewZeebeKeyRepository(db),
	}
}

// ── Topology ────────────────────────────────────────────────────────────

func (s *Server) Topology(ctx context.Context, req *pb.TopologyRequest) (*pb.TopologyResponse, error) {
	host, _ := os.Hostname()
	return &pb.TopologyResponse{
		Brokers: []*pb.BrokerInfo{{
			NodeId:  0,
			Host:    host,
			Port:    26500,
			Version: "goflow-dev",
			Partitions: []*pb.Partition{{
				PartitionId: 1,
				Role:        pb.PartitionBrokerRole_LEADER,
				Health:      pb.PartitionBrokerHealth_HEALTHY,
			}},
		}},
		ClusterSize:       1,
		PartitionsCount:   1,
		ReplicationFactor: 1,
		GatewayVersion:    "goflow-dev",
	}, nil
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

	var graph engine.ProcessGraph
	if err := json.Unmarshal(def.ParsedGraph, &graph); err != nil {
		return nil, status.Error(codes.Internal, "failed to parse process graph")
	}

	tenantID := req.TenantId
	instanceID, execID, err := service.StartProcessInstance(s.db, &graph, def.ID, &tenantID, vars)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	rt := runtime.NewRuntime(&graph, s.db, s.dispatcher)
	if err := rt.ExecuteExecution(ctx, execID); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to execute process: %v", err)
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
	activateJobsPollInterval   = 750 * time.Millisecond
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
		case <-time.After(activateJobsPollInterval):
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
		ElementInstanceKey:       instKey,
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

	result, err := service.CompleteJob(s.db, jobID, vars)
	if err != nil {
		return nil, mapJobError(err)
	}

	if !result.ProcessCompleted {
		rt := runtime.NewRuntime(result.Graph, s.db, s.dispatcher)
		if err := rt.ExecuteExecution(ctx, result.ExecutionID); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to resume process execution: %v", err)
		}
		if exec, err := s.engineRepo.GetExecutionByID(result.ExecutionID); err == nil && exec != nil && exec.ParentExecutionID != nil {
			rt.OnMultiInstanceChildCompleted(ctx, exec.ID, map[string]interface{}{})
		}
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
	result, err := service.FailJob(s.db, jobID, int(req.Retries), req.ErrorMessage, "")
	if err != nil {
		return nil, mapJobError(err)
	}

	if result.BoundaryTriggered {
		rt := runtime.NewRuntime(result.Graph, s.db, s.dispatcher)
		if err := rt.ExecuteExecution(ctx, result.ExecutionID); err != nil {
			// Non-fatal: the failure itself was already durably recorded.
			_ = err
		}
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
