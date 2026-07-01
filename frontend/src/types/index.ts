export interface ProcessDefinition {
  id: string;
  key: string;
  name: string;
  version: number;
  deploymentId: string;
  deployedAt: string;
  resource: string;
  tenantId?: string;
  historyTimeToLive?: number;
}

export interface ProcessInstance {
  id: string;
  processDefinitionId: string;
  processDefinitionKey: string;
  processDefinitionName?: string;
  businessKey?: string;
  status: 'running' | 'completed' | 'suspended' | 'terminated';
  startTime: string;
  endTime?: string;
  durationInMillis?: number;
}

export interface HistoricProcessInstance extends ProcessInstance {
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

export interface UserTask {
  id: string;
  name: string;
  assignee?: string;
  created: string;
  due?: string;
  processInstanceId: string;
  processDefinitionId: string;
  processDefinitionKey?: string;
  taskDefinitionKey: string;
  candidateGroups?: string[];
  priority?: number;
  description?: string;
}

export interface Incident {
  id: string;
  processDefinitionId: string;
  processInstanceId: string;
  executionId: string;
  incidentType: string;
  activityId: string;
  failedActivityId?: string;
  causeIncidentId?: string;
  rootCauseIncidentId?: string;
  configuration?: string;
  incidentMessage?: string;
  incidentTimestamp?: string;
  jobDefinitionId?: string;
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
  firstName?: string;
  lastName?: string;
  roles?: string[];
}

export interface TokenState {
  elementId: string;
  status: 'active' | 'completed' | 'canceled';
}
