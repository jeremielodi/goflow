import { apiClient } from './client';

export interface ProcessPermission {
  id: string;
  resourceType: string;
  processKey: string;
  granteeType: 'user' | 'role';
  granteeId: string;
  permission: 'VIEW' | 'START' | 'MANAGE';
  createdAt: string;
}

export const listProcessPermissions = async (processKey: string): Promise<ProcessPermission[]> => {
  const res = await apiClient.get(`/engine-rest/process-definition/key/${processKey}/permissions`);
  return res.data.permissions ?? [];
};

export const grantProcessPermission = async (
  processKey: string,
  params: { granteeType: 'user' | 'role'; granteeId: string; permission: 'VIEW' | 'START' | 'MANAGE' }
): Promise<ProcessPermission> => {
  const res = await apiClient.post(`/engine-rest/process-definition/key/${processKey}/permissions`, params);
  return res.data.permission;
};

export const revokeProcessPermission = async (processKey: string, id: string): Promise<void> => {
  await apiClient.delete(`/engine-rest/process-definition/key/${processKey}/permissions/${id}`);
};
