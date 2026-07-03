import { NavLink } from 'react-router-dom';
import {
  LayoutDashboard, GitBranch, Play, CheckSquare,
  AlertTriangle, History, LogOut, Workflow, Users, Shield, BarChart3
} from 'lucide-react';
import { useAuth } from '../../hooks/useAuth';

const navItems = [
  { to: '/',           icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/processes',  icon: GitBranch,        label: 'Definitions' },
  { to: '/instances',  icon: Play,             label: 'Instances' },
  { to: '/tasks',      icon: CheckSquare,      label: 'Tasks' },
  { to: '/incidents',  icon: AlertTriangle,    label: 'Incidents' },
  { to: '/history',    icon: History,          label: 'History' },
  { to: '/analytics',  icon: BarChart3,        label: 'Analytics' },
];

const adminNavItems = [
  { to: '/admin/users', icon: Users,  label: 'Users' },
  { to: '/admin/roles', icon: Shield, label: 'Roles' },
];

function NavItems({ items }: { items: typeof navItems }) {
  return (
    <>
      {items.map(({ to, icon: Icon, label }) => (
        <NavLink
          key={to}
          to={to}
          end={to === '/'}
          className={({ isActive }) =>
            `flex items-center gap-3 mx-2 px-3 py-2.5 rounded-lg text-sm transition-colors ${
              isActive
                ? 'bg-blue-600 text-white font-medium'
                : 'text-gray-400 hover:bg-gray-800 hover:text-white'
            }`
          }
        >
          <Icon className="w-4 h-4 flex-shrink-0" />
          {label}
        </NavLink>
      ))}
    </>
  );
}

export function Sidebar() {
  const { user, logout, hasAction } = useAuth();
  const showAdmin = hasAction('CAN_MANGE_USER') || hasAction('CAN_EDIT_ROLE');

  return (
    <aside className="w-56 flex-shrink-0 bg-gray-900 flex flex-col h-screen sticky top-0">
      {/* Logo */}
      <div className="flex items-center gap-2.5 px-5 py-5 border-b border-gray-800">
        <Workflow className="w-6 h-6 text-blue-400" />
        <span className="text-white font-bold text-base tracking-tight">GoFlow</span>
      </div>

      {/* Nav */}
      <nav className="flex-1 py-4 overflow-y-auto">
        <NavItems items={navItems} />
        {showAdmin && (
          <>
            <p className="mt-4 mb-1 px-5 text-xs font-semibold text-gray-600 uppercase tracking-wider">Admin</p>
            <NavItems items={adminNavItems} />
          </>
        )}
      </nav>

      {/* User */}
      <div className="border-t border-gray-800 p-4">
        <div className="flex items-center gap-3 mb-3">
          <div className="w-8 h-8 rounded-full bg-blue-500 flex items-center justify-center text-white text-xs font-bold">
            {(user?.full_name?.[0] ?? user?.email?.[0] ?? '?').toUpperCase()}
          </div>
          <div className="min-w-0">
            <p className="text-white text-xs font-medium truncate">
              {user?.full_name || user?.email}
            </p>
            <p className="text-gray-500 text-xs truncate">{user?.email}</p>
          </div>
        </div>
        <button
          onClick={logout}
          className="flex items-center gap-2 text-gray-400 hover:text-white text-xs w-full"
        >
          <LogOut className="w-3.5 h-3.5" /> Sign out
        </button>
      </div>
    </aside>
  );
}
