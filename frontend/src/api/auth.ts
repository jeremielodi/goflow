import { apiClient } from './client';

export interface LoginResponse {
  access_token: string;
  refresh_token: string;
  user: { id: string; email: string; full_name?: string };
}

export const login = async (email: string, password: string): Promise<LoginResponse> => {
  const res = await apiClient.post('/auth/login', { email, password });
  return res.data;
};

export const logout = async () => {
  await apiClient.post('/auth/logout');
};

// GET /users/me returns { user, roles, actions }, not a flat user object.
export const getMe = async (): Promise<{ user: { id: string; email: string; full_name?: string }; roles: unknown[]; actions: unknown[] }> => {
  const res = await apiClient.get('/users/me');
  return res.data;
};
