import { apiClient } from './client';
import type { UserTask } from '../types';

export const listTasks = async (params?: {
  processInstanceId?: string;
  assignee?: string;
  firstResult?: number;
  maxResults?: number;
}): Promise<UserTask[]> => {
  const res = await apiClient.get('/engine-rest/tasks', { params });
  return Array.isArray(res.data) ? res.data : res.data?.items ?? [];
};

export const completeTask = async (
  id: string,
  variables?: Record<string, unknown>
): Promise<void> => {
  await apiClient.post(`/engine-rest/tasks/${id}/complete`, { variables });
};

export const claimTask = async (id: string, userId: string): Promise<void> => {
  await apiClient.post(`/engine-rest/tasks/${id}/claim`, { userId });
};

export const unclaimTask = async (id: string): Promise<void> => {
  await apiClient.post(`/engine-rest/tasks/${id}/unclaim`);
};
