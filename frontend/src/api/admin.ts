import { apiClient } from './client';

// ── Types ────────────────────────────────────────────────────────────────

export interface AdminUser {
  id: string;
  email: string;
  full_name?: string;
  is_active: boolean;
  tenantId?: string;
  created_at: string;
  updated_at: string;
}

export interface Role {
  id: string;
  label: string;
  is_default: number;
}

export interface Action {
  id: number;
  code: string;
  description: string;
}

// ── Users ────────────────────────────────────────────────────────────────

export const listUsers = async (): Promise<AdminUser[]> => {
  const res = await apiClient.get('/users');
  return res.data.users ?? [];
};

export const getUser = async (id: string): Promise<{ user: AdminUser; roles: Role[] }> => {
  const res = await apiClient.get(`/users/${id}`);
  return res.data;
};

export const createUser = async (params: {
  email: string;
  password: string;
  full_name?: string;
  tenant_id?: string;
  role_ids?: string[];
}): Promise<AdminUser> => {
  const res = await apiClient.post('/users', params);
  return res.data.user;
};

export const updateUser = async (
  id: string,
  params: { email: string; full_name?: string; is_active: boolean }
): Promise<void> => {
  await apiClient.put(`/users/${id}`, params);
};

export const deleteUser = async (id: string): Promise<void> => {
  await apiClient.delete(`/users/${id}`);
};

export const getUserRoles = async (userId: string): Promise<Role[]> => {
  const res = await apiClient.get(`/users/${userId}/roles`);
  return res.data.roles ?? [];
};

export const assignRoleToUser = async (userId: string, roleId: string): Promise<void> => {
  await apiClient.post(`/users/${userId}/roles/${roleId}`);
};

export const removeRoleFromUser = async (userId: string, roleId: string): Promise<void> => {
  await apiClient.delete(`/users/${userId}/roles/${roleId}`);
};

// ── Roles ────────────────────────────────────────────────────────────────

export const listRoles = async (): Promise<Role[]> => {
  const res = await apiClient.get('/roles');
  return res.data.roles ?? [];
};

export const createRole = async (label: string): Promise<Role> => {
  const res = await apiClient.post('/roles', { label, is_default: 0 });
  return res.data.role;
};

export const updateRole = async (id: string, label: string, isDefault = 0): Promise<void> => {
  await apiClient.put(`/roles/${id}`, { label, is_default: isDefault });
};

export const deleteRole = async (id: string): Promise<void> => {
  await apiClient.delete(`/roles/${id}`);
};

// ── Actions ──────────────────────────────────────────────────────────────

export const listActions = async (): Promise<Action[]> => {
  const res = await apiClient.get('/actions');
  return res.data.actions ?? [];
};

export const getRoleActions = async (roleId: string): Promise<Action[]> => {
  const res = await apiClient.get(`/roles/${roleId}/actions`);
  return res.data.actions ?? [];
};

export const assignActionToRole = async (roleId: string, actionId: number): Promise<void> => {
  await apiClient.post(`/roles/${roleId}/actions/${actionId}`);
};

export const removeActionFromRole = async (roleId: string, actionId: number): Promise<void> => {
  await apiClient.delete(`/roles/${roleId}/actions/${actionId}`);
};
