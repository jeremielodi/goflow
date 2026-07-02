import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { AuthProvider, useAuth } from './useAuth';
import * as authApi from '../api/auth';

// Regression test for a real bug: GET /users/me returns { user, roles, actions },
// not a flat user object. AuthProvider must unwrap `.user` on session restore,
// otherwise every `user?.email`/`user?.full_name` read downstream silently
// breaks after a page refresh (while a fresh login still works, since the
// login response shape is already flat).
vi.mock('../api/auth');

function Probe() {
  const { user, isLoading } = useAuth();
  if (isLoading) return <div>loading</div>;
  return <div>{user ? `signed-in:${user.email}` : 'signed-out'}</div>;
}

describe('AuthProvider session restore', () => {
  beforeEach(() => {
    localStorage.clear();
    vi.resetAllMocks();
  });

  it('unwraps the nested user object returned by GET /users/me', async () => {
    localStorage.setItem('accessToken', 'token-123');
    vi.mocked(authApi.getMe).mockResolvedValue({
      user: { id: 'u1', email: 'admin@goflow.com', full_name: 'Admin User' },
      roles: ['admin'],
      actions: ['CAN_DO_EVERYTHING'],
    });

    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>
    );

    await waitFor(() => {
      expect(screen.getByText('signed-in:admin@goflow.com')).toBeInTheDocument();
    });
  });

  it('signs out when no token is stored, without calling getMe', async () => {
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>
    );

    await waitFor(() => {
      expect(screen.getByText('signed-out')).toBeInTheDocument();
    });
    expect(authApi.getMe).not.toHaveBeenCalled();
  });
});
