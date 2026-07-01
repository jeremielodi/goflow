import { apiClient } from './client';
import type { Incident } from '../types';

export const listIncidents = async (params?: {
  processInstanceId?: string;
}): Promise<Incident[]> => {
  const res = await apiClient.get('/engine-rest/incident', { params });
  return Array.isArray(res.data) ? res.data : res.data?.incidents ?? [];
};

export const deleteIncident = async (id: string): Promise<void> => {
  await apiClient.delete(`/engine-rest/incident/${id}`);
};

export const retryJob = async (jobId: string, retries = 3): Promise<void> => {
  await apiClient.post(`/engine-rest/job/${jobId}/retries`, { retries });
};
