export interface ProcessDefinition {
  id: string;
  key: string;
  name?: string;
  version: number;
  deploymentId: string;
  createdAt: string;
  isActive?: boolean;
  tenantId?: string;
}

// GET /engine-rest/process-instance(/:id) returns internal/models.ProcessInstance
// marshaled as-is — field names below match that JSON shape, which differs
// from the Camunda-7-style HistoricProcessInstance shape below (that one
// comes from a dedicated history_controller.go response struct).
export interface ProcessInstance {
  id: string;
  processDefinitionId: string;
  status: 'running' | 'completed' | 'suspended' | 'terminated';
  startedBy?: string;
  startedAt: string;
  endedAt?: string;
  processKey?: string;
  processName?: string;
  version?: number;
  tenantId?: string;
}

// GET /audit/process/:id/token-history returns internal/service.TokenHistoryStep[]
// (see token-history-service.go) — a derived, ordered timeline reconstructed
// from audit_logs, unlike HistoricActivityInstance below which reflects only
// the *current* state of the live executions table.
export interface TokenHistoryStep {
  timestamp: string;
  action: string;
  elementId?: string;
  elementName?: string;
  executionId?: string;
  taskId?: string;
  detail?: string;
}

export interface HistoricProcessInstance {
  id: string;
  processDefinitionId: string;
  processDefinitionKey: string;
  businessKey?: string;
  startTime: string;
  endTime?: string;
  durationInMillis?: number;
  state: 'active' | 'completed' | 'suspended' | 'terminated';
  startUserId?: string;
}

export interface HistoricActivityInstance {
  id: string;
  activityId: string;
  activityName?: string;
  activityType: string;
  processInstanceId: string;
  processDefinitionId: string;
  startTime: string;
  endTime?: string;
  durationInMillis?: number;
  canceled: boolean;
  completeScope: boolean;
}

// Mirrors the database's task_status enum (database/01_schema.sql) exactly.
export type TaskStatus = 'created' | 'claimed' | 'completed' | 'cancelled';

export interface UserTask {
  id: string;
  // The GoFlow /engine-rest/tasks response is models.Task marshaled as-is
  // (see internal/models/task.go) — field names below match that JSON
  // shape exactly, not the Camunda 7 REST API's naming.
  taskName?: string;
  assignee?: string;
  status: TaskStatus;
  createdAt: string;
  due?: string;
  processInstanceId: string;
  processDefinitionId?: string;
  processDefinitionKey?: string;
  taskDefinitionKey: string;
  candidateGroup?: string;
  priority?: number;
  description?: string;
  formKey?: string;
}

export interface Incident {
  id: string;
  processInstanceId: string;
  jobId?: string;
  incidentType: string;
  activityId: string;
  errorMessage?: string;
  errorCode?: string;
  state: string;
  createdAt: string;
  resolvedAt?: string;
}

export interface ProcessKeyStats {
  processKey: string;
  runningCount: number;
  completedCount: number;
  terminatedCount: number;
  avgDurationMillis?: number | null;
  p50DurationMillis?: number | null;
  p95DurationMillis?: number | null;
  incidentCount: number;
  incidentRate: number;
}

export interface ThroughputPoint {
  date: string;
  started: number;
  completed: number;
  terminated: number;
}

export interface ProcessStats {
  byProcessKey: ProcessKeyStats[];
  throughput: ThroughputPoint[];
}

export interface Variable {
  value: unknown;
  type: string;
}

export type Variables = Record<string, Variable>;

export interface Job {
  id: string;
  processInstanceId: string;
  processDefinitionId: string;
  elementId: string;
  type: string;
  topic?: string;
  retries: number;
  errorMessage?: string;
  lockOwner?: string;
  lockExpirationTime?: string;
  createdAt: string;
  status: 'pending' | 'locked' | 'completed' | 'failed';
}

export interface User {
  id: string;
  email: string;
  fullName?: string;
  roles?: string[];
}

export interface TokenState {
  elementId: string;
  status: 'active' | 'completed' | 'canceled';
}
