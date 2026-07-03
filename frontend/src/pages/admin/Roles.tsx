import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { ShieldPlus, Trash2, Settings } from 'lucide-react';
import {
  listRoles, createRole, updateRole, deleteRole,
  listActions, getRoleActions, assignActionToRole, removeActionFromRole,
} from '../../api/admin';
import type { Role } from '../../api/admin';
import { Card, Table, Thead, Th, Tr, Td, Badge, Button, Modal, Input, Spinner, EmptyState } from '../../components/ui';

export default function AdminRoles() {
  const qc = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [newLabel, setNewLabel] = useState('');
  const [manageRole, setManageRole] = useState<Role | null>(null);

  const { data: roles = [], isLoading } = useQuery({ queryKey: ['admin-roles'], queryFn: listRoles });

  const createMut = useMutation({
    mutationFn: () => createRole(newLabel),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['admin-roles'] });
      setNewLabel('');
      setCreateOpen(false);
    },
  });

  const deleteMut = useMutation({
    mutationFn: deleteRole,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['admin-roles'] }),
  });

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Roles</h1>
          <p className="text-sm text-gray-500 mt-0.5">{roles.length} roles</p>
        </div>
        <Button size="sm" onClick={() => setCreateOpen(true)}>
          <ShieldPlus className="w-4 h-4" /> New Role
        </Button>
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner className="w-6 h-6 text-blue-500" /></div>
        ) : roles.length === 0 ? (
          <EmptyState title="No roles" description="Create the first role to get started" />
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>Label</Th>
                <Th>Default</Th>
                <Th></Th>
              </tr>
            </Thead>
            <tbody>
              {roles.map(r => (
                <Tr key={r.id}>
                  <Td className="font-medium text-gray-900">{r.label}</Td>
                  <Td>{r.is_default === 1 ? <Badge value="active" label="Default" /> : '—'}</Td>
                  <Td>
                    <div className="flex items-center gap-2 justify-end">
                      <Button variant="secondary" size="sm" onClick={() => setManageRole(r)}>
                        <Settings className="w-3.5 h-3.5" /> Actions
                      </Button>
                      <Button
                        variant="ghost" size="sm"
                        onClick={() => { if (confirm(`Delete role ${r.label}?`)) deleteMut.mutate(r.id); }}
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

      <Modal open={createOpen} onClose={() => setCreateOpen(false)} title="New Role">
        <div className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1.5">Label</label>
            <Input value={newLabel} onChange={e => setNewLabel(e.target.value)} />
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button onClick={() => createMut.mutate()} disabled={!newLabel || createMut.isPending}>
              {createMut.isPending && <Spinner className="w-4 h-4 text-white" />}
              Create
            </Button>
          </div>
        </div>
      </Modal>

      {manageRole && (
        <ManageActionsModal role={manageRole} onClose={() => setManageRole(null)} />
      )}
    </div>
  );
}

function ManageActionsModal({ role, onClose }: { role: Role; onClose: () => void }) {
  const qc = useQueryClient();
  const { data: allActions = [] } = useQuery({ queryKey: ['admin-actions'], queryFn: listActions });
  const { data: roleActions = [] } = useQuery({
    queryKey: ['admin-role-actions', role.id],
    queryFn: () => getRoleActions(role.id),
  });

  const toggleMut = useMutation({
    mutationFn: async ({ actionId, enabled }: { actionId: number; enabled: boolean }) =>
      enabled ? assignActionToRole(role.id, actionId) : removeActionFromRole(role.id, actionId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['admin-role-actions', role.id] }),
  });

  const enabledIds = new Set(roleActions.map(a => a.id));

  return (
    <Modal open onClose={onClose} title={`Actions for ${role.label}`}>
      <div className="space-y-1 max-h-96 overflow-y-auto">
        {allActions.map(a => (
          <label key={a.id} className="flex items-center gap-2 text-sm text-gray-700 py-1">
            <input
              type="checkbox"
              checked={enabledIds.has(a.id)}
              onChange={e => toggleMut.mutate({ actionId: a.id, enabled: e.target.checked })}
            />
            <div>
              <span className="font-mono text-xs text-gray-900">{a.code}</span>
              <span className="text-xs text-gray-400 ml-2">{a.description}</span>
            </div>
          </label>
        ))}
      </div>
      <div className="flex justify-end pt-4">
        <Button variant="secondary" onClick={onClose}>Close</Button>
      </div>
    </Modal>
  );
}
