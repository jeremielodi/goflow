import { apiClient } from './client';
import type { ProcessDefinition, ProcessInstance, Variables } from '../types';

// ── Process Definitions ────────────────────────────────────────────────────

export const listDefinitions = async (params?: {
  key?: string;
  latestVersion?: boolean;
}): Promise<ProcessDefinition[]> => {
  const res = await apiClient.get('/engine-rest/process-definition', { params });
  return res.data;
};

export const getDefinition = async (id: string): Promise<ProcessDefinition> => {
  const res = await apiClient.get(`/engine-rest/process-definition/${id}`);
  return res.data;
};

export interface TokenCount {
  elementId: string;
  count: number;
}

// Live per-node active-token counts across every currently running instance
// of a process key — a snapshot ("where are running instances sitting right
// now"), not a historical heatmap of everywhere instances have ever been.
export const getActiveTokenCounts = async (key: string): Promise<TokenCount[]> => {
  const res = await apiClient.get(`/engine-rest/process-definition/key/${key}/token-counts`);
  return res.data;
};

export interface InstancePosition {
  processInstanceId: string;
  elementId: string;
}

// Per-instance breakdown of getActiveTokenCounts — one row per (instance,
// active token) pair, so an instance with several concurrent tokens
// (parallel gateway, multi-instance) has several entries.
export const getInstancePositions = async (key: string): Promise<InstancePosition[]> => {
  const res = await apiClient.get(`/engine-rest/process-definition/key/${key}/instance-positions`);
  return res.data;
};

export const deployBpmn = async (file: File, name?: string): Promise<void> => {
  const form = new FormData();
  form.append('file', file);
  if (name) form.append('deployment-name', name);
  await apiClient.post('/engine-rest/deployment/create', form, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
};

// ── Process Instances ──────────────────────────────────────────────────────

export const listInstances = async (params?: {
  processKey?: string;
  status?: string;
  firstResult?: number;
  maxResults?: number;
}): Promise<ProcessInstance[]> => {
  const res = await apiClient.get('/engine-rest/process-instance', { params });
  return Array.isArray(res.data) ? res.data : res.data?.items ?? [];
};

export const getInstance = async (id: string): Promise<ProcessInstance> => {
  const res = await apiClient.get(`/engine-rest/process-instance/${id}`);
  return res.data;
};

export const getInstanceVariables = async (id: string): Promise<Variables> => {
  const res = await apiClient.get(`/engine-rest/process-instance/${id}/variables`);
  return res.data;
};

export const startProcess = async (
  key: string,
  variables?: Record<string, unknown>
): Promise<{ processInstanceId: string }> => {
  const res = await apiClient.post(`/engine-rest/v2/process-definitions/${key}/start`, { variables });
  return res.data;
};

export const terminateInstance = async (id: string): Promise<void> => {
  await apiClient.delete(`/engine-rest/process-instance/${id}`);
};

export const suspendInstance = async (id: string, suspended: boolean): Promise<void> => {
  await apiClient.put(`/engine-rest/process-instance/${id}/suspended`, { suspended });
};

export const getInstanceState = async (id: string) => {
  const res = await apiClient.get(`/engine-rest/process-instance/${id}/state`);
  return res.data;
};

// Moves a running token from sourceElementId to targetElementId — see
// internal/runtime/modification.go. Cancels the source execution (and any
// open task/job attached to it) and creates a fresh one at the target.
export const modifyInstance = async (
  id: string,
  sourceElementId: string,
  targetElementId: string
): Promise<{ newExecutionIds: string[] }> => {
  const res = await apiClient.post(`/v2/process-instances/${id}/modification`, {
    moveInstructions: [{ sourceElementId, targetElementId }],
  });
  return res.data;
};

export interface StepBackResult {
  executionId: string;
  fromElementId: string;
  toElementId: string;
  toElementName?: string;
  applied: boolean;
  cancelledTaskIds?: string[];
}

// The "previous" button: sends a token back to the wait state it went
// through before — see internal/runtime/step_back.go. With dryRun the
// backend only says where the token would go (or why it can't).
export const stepBackInstance = async (
  id: string,
  executionId: string,
  dryRun = false
): Promise<StepBackResult> => {
  const res = await apiClient.post(`/v2/process-instances/${id}/step-back`, { executionId, dryRun });
  return res.data;
};
