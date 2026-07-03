import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AuthProvider, useAuth } from './hooks/useAuth';
import { Layout } from './components/layout/Layout';
import Login from './pages/Login';
import Dashboard from './pages/Dashboard';
import ProcessDefinitions from './pages/ProcessDefinitions';
import ProcessInstances from './pages/ProcessInstances';
import InstanceDetail from './pages/InstanceDetail';
import Tasks from './pages/Tasks';
import Incidents from './pages/Incidents';
import History from './pages/History';
import AdminUsers from './pages/admin/Users';
import AdminRoles from './pages/admin/Roles';
import { Spinner } from './components/ui';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 10_000,
    },
  },
});

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { user, isLoading } = useAuth();
  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <Spinner className="w-8 h-8 text-blue-500" />
      </div>
    );
  }
  if (!user) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/login" element={<Login />} />
            <Route
              element={
                <ProtectedRoute>
                  <Layout />
                </ProtectedRoute>
              }
            >
              <Route path="/"              element={<Dashboard />} />
              <Route path="/processes"     element={<ProcessDefinitions />} />
              <Route path="/instances"     element={<ProcessInstances />} />
              <Route path="/instances/:id" element={<InstanceDetail />} />
              <Route path="/tasks"         element={<Tasks />} />
              <Route path="/incidents"     element={<Incidents />} />
              <Route path="/history"       element={<History />} />
              <Route path="/admin/users"   element={<AdminUsers />} />
              <Route path="/admin/roles"   element={<AdminRoles />} />
              <Route path="*"              element={<Navigate to="/" replace />} />
            </Route>
          </Routes>
        </BrowserRouter>
      </AuthProvider>
    </QueryClientProvider>
  );
}
