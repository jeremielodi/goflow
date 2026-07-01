import { apiClient } from './client';

export interface LoginResponse {
  access_token: string;
  refresh_token: string;
  user: { id: string; email: string; firstName?: string; lastName?: string };
}

export const login = async (email: string, password: string): Promise<LoginResponse> => {
  const res = await apiClient.post('/auth/login', { email, password });
  return res.data;
};

export const logout = async () => {
  await apiClient.post('/auth/logout');
};

export const getMe = async () => {
  const res = await apiClient.get('/users/me');
  return res.data;
};
