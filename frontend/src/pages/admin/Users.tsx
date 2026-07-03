import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { UserPlus, Trash2, Pencil } from 'lucide-react';
import {
  listUsers, createUser, updateUser, deleteUser,
  listRoles, getUserRoles, assignRoleToUser, removeRoleFromUser,
} from '../../api/admin';
import type { AdminUser } from '../../api/admin';
import { Card, Table, Thead, Th, Tr, Td, Badge, Button, Modal, Input, Spinner, EmptyState } from '../../components/ui';
import { formatDate } from '../../utils/format';

export default function AdminUsers() {
  const qc = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [editUser, setEditUser] = useState<AdminUser | null>(null);

  const { data: users = [], isLoading } = useQuery({ queryKey: ['admin-users'], queryFn: listUsers });
  const { data: roles = [] } = useQuery({ queryKey: ['admin-roles'], queryFn: listRoles });

  const deleteMut = useMutation({
    mutationFn: deleteUser,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['admin-users'] }),
  });

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Users</h1>
          <p className="text-sm text-gray-500 mt-0.5">{users.length} users</p>
        </div>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <UserPlus className="w-4 h-4" /> New User
        </Button>
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner className="w-6 h-6 text-blue-500" /></div>
        ) : users.length === 0 ? (
          <EmptyState title="No users" description="Create the first user to get started" />
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>Email</Th>
                <Th>Full Name</Th>
                <Th>Tenant</Th>
                <Th>Status</Th>
                <Th>Created</Th>
                <Th></Th>
              </tr>
            </Thead>
            <tbody>
              {users.map(u => (
                <Tr key={u.id}>
                  <Td className="font-medium text-gray-900">{u.email}</Td>
                  <Td>{u.full_name || '—'}</Td>
                  <Td>{u.tenantId || '—'}</Td>
                  <Td><Badge value={u.is_active ? 'active' : 'suspended'} label={u.is_active ? 'Active' : 'Inactive'} /></Td>
                  <Td><span className="text-xs">{formatDate(u.created_at)}</span></Td>
                  <Td>
                    <div className="flex items-center gap-2 justify-end">
                      <Button variant="ghost" size="sm" onClick={() => setEditUser(u)}>
                        <Pencil className="w-3.5 h-3.5" />
                      </Button>
                      <Button
                        variant="ghost" size="sm"
                        onClick={() => { if (confirm(`Delete user ${u.email}?`)) deleteMut.mutate(u.id); }}
                      >
                        <Trash2 className="w-3.5 h-3.5 text-red-500" />
                      </Button>
                    </div>
                  </Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        )}
      </Card>

      <CreateUserModal open={createOpen} onClose={() => setCreateOpen(false)} roles={roles} />
      {editUser && (
        <EditUserModal user={editUser} roles={roles} onClose={() => setEditUser(null)} />
      )}
    </div>
  );
}

function CreateUserModal({ open, onClose, roles }: {
  open: boolean; onClose: () => void; roles: { id: string; label: string }[];
}) {
  const qc = useQueryClient();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState('');
  const [tenantId, setTenantId] = useState('');
  const [roleIds, setRoleIds] = useState<string[]>([]);
  const [error, setError] = useState('');

  const mut = useMutation({
    mutationFn: () => createUser({
      email, password, full_name: fullName || undefined,
      tenant_id: tenantId || undefined, role_ids: roleIds.length ? roleIds : undefined,
    }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['admin-users'] });
      reset();
      onClose();
    },
    onError: (e: unknown) => setError(e instanceof Error ? e.message : 'Failed to create user'),
  });

  const reset = () => { setEmail(''); setPassword(''); setFullName(''); setTenantId(''); setRoleIds([]); setError(''); };

  const toggleRole = (id: string) =>
    setRoleIds(prev => prev.includes(id) ? prev.filter(r => r !== id) : [...prev, id]);

  return (
    <Modal open={open} onClose={() => { reset(); onClose(); }} title="New User">
      <div className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1.5">Email</label>
          <Input type="email" value={email} onChange={e => setEmail(e.target.value)} />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1.5">Password</label>
          <Input type="password" value={password} onChange={e => setPassword(e.target.value)} />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1.5">Full Name</label>
          <Input value={fullName} onChange={e => setFullName(e.target.value)} />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1.5">Tenant ID (optional)</label>
          <Input value={tenantId} onChange={e => setTenantId(e.target.value)} />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1.5">Roles</label>
          <div className="space-y-1">
            {roles.map(r => (
              <label key={r.id} className="flex items-center gap-2 text-sm text-gray-700">
                <input type="checkbox" checked={roleIds.includes(r.id)} onChange={() => toggleRole(r.id)} />
                {r.label}
              </label>
            ))}
          </div>
        </div>
        {error && <p className="text-xs text-red-500">{error}</p>}
        <div className="flex justify-end gap-2 pt-2">
          <Button variant="secondary" onClick={() => { reset(); onClose(); }}>Cancel</Button>
          <Button onClick={() => mut.mutate()} disabled={!email || !password || mut.isPending}>
            {mut.isPending && <Spinner className="w-4 h-4 text-white" />}
            Create
          </Button>
        </div>
      </div>
    </Modal>
  );
}

function EditUserModal({ user, roles, onClose }: {
  user: AdminUser; roles: { id: string; label: string }[]; onClose: () => void;
}) {
  const qc = useQueryClient();
  const [email, setEmail] = useState(user.email);
  const [fullName, setFullName] = useState(user.full_name ?? '');
  const [isActive, setIsActive] = useState(user.is_active);

  const { data: currentRoles = [] } = useQuery({
    queryKey: ['admin-user-roles', user.id],
    queryFn: () => getUserRoles(user.id),
  });
  const [roleIds, setRoleIds] = useState<string[] | null>(null);
  const effectiveRoleIds = roleIds ?? currentRoles.map(r => r.id);

  const saveMut = useMutation({
    mutationFn: async () => {
      await updateUser(user.id, { email, full_name: fullName, is_active: isActive });
      const before = new Set(currentRoles.map(r => r.id));
      const after = new Set(effectiveRoleIds);
      for (const id of after) if (!before.has(id)) await assignRoleToUser(user.id, id);
      for (const id of before) if (!after.has(id)) await removeRoleFromUser(user.id, id);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['admin-users'] });
      qc.invalidateQueries({ queryKey: ['admin-user-roles', user.id] });
      onClose();
    },
  });

  const toggleRole = (id: string) => {
    const next = new Set(effectiveRoleIds);
    if (next.has(id)) next.delete(id); else next.add(id);
    setRoleIds([...next]);
  };

  return (
    <Modal open onClose={onClose} title={`Edit ${user.email}`}>
      <div className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1.5">Email</label>
          <Input type="email" value={email} onChange={e => setEmail(e.target.value)} />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1.5">Full Name</label>
          <Input value={fullName} onChange={e => setFullName(e.target.value)} />
        </div>
        <label className="flex items-center gap-2 text-sm text-gray-700">
          <input type="checkbox" checked={isActive} onChange={e => setIsActive(e.target.checked)} />
          Active
        </label>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1.5">Roles</label>
          <div className="space-y-1">
            {roles.map(r => (
              <label key={r.id} className="flex items-center gap-2 text-sm text-gray-700">
                <input type="checkbox" checked={effectiveRoleIds.includes(r.id)} onChange={() => toggleRole(r.id)} />
                {r.label}
              </label>
            ))}
          </div>
        </div>
        <div className="flex justify-end gap-2 pt-2">
          <Button variant="secondary" onClick={onClose}>Cancel</Button>
          <Button onClick={() => saveMut.mutate()} disabled={saveMut.isPending}>
            {saveMut.isPending && <Spinner className="w-4 h-4 text-white" />}
            Save
          </Button>
        </div>
      </div>
    </Modal>
  );
}
