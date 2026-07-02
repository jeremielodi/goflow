import { apiClient } from './client';
import type { TokenHistoryStep } from '../types';

export const getTokenHistory = async (processInstanceId: string): Promise<TokenHistoryStep[]> => {
  const res = await apiClient.get(`/engine-rest/audit/process/${processInstanceId}/token-history`, { });
  return res.data.steps ?? [];
};
