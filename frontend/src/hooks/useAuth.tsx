import { createContext, useContext, useState, useEffect } from 'react';
import type { ReactNode } from 'react';
import { login as apiLogin, logout as apiLogout, getMe } from '../api/auth';

interface AuthUser {
  id: string;
  email: string;
  full_name?: string;
}

interface AuthContextValue {
  user: AuthUser | null;
  roles: string[];
  actions: string[];
  isLoading: boolean;
  login: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  hasAction: (code: string) => boolean;
}

const AuthContext = createContext<AuthContextValue | null>(null);

// GetCurrentUser (/users/me) returns roles/actions as full objects
// ({label}/{code}), while Login returns them already flattened to
// string[] — normalize both shapes here.
function toCodes(items: unknown[] | undefined, key: 'label' | 'code'): string[] {
  if (!items) return [];
  return items.map(item => (typeof item === 'string' ? item : (item as Record<string, string>)[key]));
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null);
  const [roles, setRoles] = useState<string[]>([]);
  const [actions, setActions] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const token = localStorage.getItem('accessToken');
    if (!token) {
      setIsLoading(false);
      return;
    }
    getMe()
      .then(res => {
        setUser(res.user);
        setRoles(toCodes(res.roles, 'label'));
        setActions(toCodes(res.actions, 'code'));
      })
      .catch(() => {
        localStorage.removeItem('accessToken');
        localStorage.removeItem('refreshToken');
      })
      .finally(() => setIsLoading(false));
  }, []);

  const login = async (email: string, password: string) => {
    const res = await apiLogin(email, password);
    localStorage.setItem('accessToken', res.access_token);
    localStorage.setItem('refreshToken', res.refresh_token);
    setUser(res.user);
    setRoles(toCodes(res.roles, 'label'));
    setActions(toCodes(res.actions, 'code'));
  };

  const logout = async () => {
    try { await apiLogout(); } catch { /* ignore */ }
    localStorage.removeItem('accessToken');
    localStorage.removeItem('refreshToken');
    setUser(null);
    setRoles([]);
    setActions([]);
  };

  const hasAction = (code: string) => actions.includes(code);

  return (
    <AuthContext.Provider value={{ user, roles, actions, isLoading, login, logout, hasAction }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
