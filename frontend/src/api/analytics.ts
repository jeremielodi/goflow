import { apiClient } from './client';
import type { ProcessStats } from '../types';

export const getProcessStats = async (params?: {
  processKey?: string;
  from?: string;
  to?: string;
}): Promise<ProcessStats> => {
  const res = await apiClient.get('/engine-rest/analytics/process-stats', { params });
  return res.data;
};
