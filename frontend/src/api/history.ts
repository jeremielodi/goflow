import { apiClient } from './client';
import type { HistoricProcessInstance, HistoricActivityInstance } from '../types';

export const listHistoricInstances = async (params?: {
  processDefinitionKey?: string;
  state?: string;
  startedAfter?: string;
  startedBefore?: string;
  firstResult?: number;
  maxResults?: number;
}): Promise<HistoricProcessInstance[]> => {
  const res = await apiClient.get('/engine-rest/history/process-instance', { params });
  return res.data;
};

export const getHistoricInstance = async (id: string): Promise<HistoricProcessInstance> => {
  const res = await apiClient.get(`/engine-rest/history/process-instance/${id}`);
  return res.data;
};

export const listHistoricActivities = async (
  processInstanceId: string
): Promise<HistoricActivityInstance[]> => {
  const res = await apiClient.get('/engine-rest/history/activity-instance', {
    params: { processInstanceId },
  });
  return res.data;
};
