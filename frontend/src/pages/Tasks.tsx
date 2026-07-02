import { useEffect, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { CheckSquare, RefreshCw, User, UserCheck } from 'lucide-react';
import { listTasks, completeTask, claimTask, unclaimTask } from '../api/tasks';
import { getForm } from '../api/forms';
import { useAuth } from '../hooks/useAuth';
import { Card, CardHeader, Table, Thead, Th, Tr, Td, Badge, Button, Modal, Spinner, EmptyState } from '../components/ui';
import { formatDate } from '../utils/format';
import type { UserTask } from '../types';

export default function Tasks() {
  const qc = useQueryClient();
  const nav = useNavigate();
  const { user } = useAuth();
  const [searchParams] = useSearchParams();

  const [assigneeFilter, setAssigneeFilter] = useState('');
  const [completeTask_, setCompleteTask_]   = useState<UserTask | null>(null);
  const [completeVars, setCompleteVars]     = useState('{}');
  const [completeError, setCompleteError]   = useState('');
  const [formValues, setFormValues]         = useState<Record<string, unknown>>({});

  const { data: form, isLoading: formLoading } = useQuery({
    queryKey: ['form', completeTask_?.formKey],
    queryFn: () => getForm(completeTask_!.formKey!),
    enabled: !!completeTask_?.formKey,
    retry: false,
  });

  // Reset the dynamic form's values whenever a new form loads for the task
  // currently being completed.
  useEffect(() => {
    if (form?.schema?.components) {
      const initial: Record<string, unknown> = {};
      for (const c of form.schema.components) {
        initial[c.key] = c.type === 'checkbox' ? false : '';
      }
      setFormValues(initial);
    }
  }, [form]);

  const instanceFilter = searchParams.get('instance') ?? undefined;

  const { data: tasks = [], isLoading, refetch } = useQuery({
    queryKey: ['tasks', assigneeFilter, instanceFilter],
    queryFn: () => listTasks({
      processInstanceId: instanceFilter,
      assignee: assigneeFilter || undefined,
      maxResults: 200,
    }),
    refetchInterval: 5000,
  });

  const completeMut = useMutation({
    mutationFn: ({ id, vars }: { id: string; vars: Record<string, unknown> }) =>
      completeTask(id, vars),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['tasks'] });
      qc.invalidateQueries({ queryKey: ['instances'] });
      setCompleteTask_(null);
    },
  });

  const claimMut = useMutation({
    mutationFn: ({ id, userId }: { id: string; userId: string }) => claimTask(id, userId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['tasks'] }),
  });

  const unclaimMut = useMutation({
    mutationFn: (id: string) => unclaimTask(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['tasks'] }),
  });

  const usingDynamicForm = !!completeTask_?.formKey && !!form?.schema?.components?.length;

  const handleComplete = () => {
    if (!completeTask_) return;
    setCompleteError('');
    if (usingDynamicForm) {
      completeMut.mutate({ id: completeTask_.id, vars: formValues });
      return;
    }
    try {
      const vars = JSON.parse(completeVars);
      completeMut.mutate({ id: completeTask_.id, vars });
    } catch {
      setCompleteError('Invalid JSON');
    }
  };

  const myTasks  = tasks.filter(t => t.assignee === user?.email);
  const allTasks = assigneeFilter ? tasks.filter(t => t.assignee === assigneeFilter) : tasks;

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">User Tasks</h1>
          <p className="text-sm text-gray-500 mt-0.5">{tasks.length} open tasks</p>
        </div>
        <Button variant="ghost" size="sm" onClick={() => refetch()}>
          <RefreshCw className="w-4 h-4" />
        </Button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-2">
        <button
          onClick={() => setAssigneeFilter('')}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
            !assigneeFilter ? 'bg-blue-600 text-white' : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50'
          }`}
        >
          All ({tasks.length})
        </button>
        <button
          onClick={() => setAssigneeFilter(user?.email ?? '')}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
            assigneeFilter === user?.email ? 'bg-blue-600 text-white' : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50'
          }`}
        >
          My Tasks ({myTasks.length})
        </button>
        <button
          onClick={() => setAssigneeFilter('__unassigned__')}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
            assigneeFilter === '__unassigned__' ? 'bg-blue-600 text-white' : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50'
          }`}
        >
          Unassigned ({tasks.filter(t => !t.assignee).length})
        </button>
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner className="w-6 h-6 text-blue-500" /></div>
        ) : allTasks.length === 0 ? (
          <EmptyState
            icon={<CheckSquare className="w-12 h-12" />}
            title="No tasks found"
            description="All caught up!"
          />
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>Task Name</Th>
                <Th>Process Instance</Th>
                <Th>Assignee</Th>
                <Th>Status</Th>
                <Th>Created</Th>
                <Th></Th>
              </tr>
            </Thead>
            <tbody>
              {allTasks.map(task => {
                const isOpen = task.status === 'created' || task.status === 'claimed';
                return (
                <Tr key={task.id}>
                  <Td>
                    <p className="font-medium text-sm">{task.taskName}</p>
                    <p className="text-xs text-gray-400">{task.taskDefinitionKey}</p>
                  </Td>
                  <Td>
                    <button
                      onClick={() => nav(`/instances/${task.processInstanceId}`)}
                      className="text-xs text-blue-600 hover:underline font-mono"
                    >
                      {task.processInstanceId?.slice(0, 8)}…
                    </button>
                  </Td>
                  <Td>
                    {task.assignee ? (
                      <span className="flex items-center gap-1 text-xs">
                        <UserCheck className="w-3.5 h-3.5 text-green-500" />
                        {task.assignee}
                      </span>
                    ) : (
                      <span className="flex items-center gap-1 text-xs text-gray-400">
                        <User className="w-3.5 h-3.5" /> Unassigned
                      </span>
                    )}
                  </Td>
                  <Td><Badge value={task.status} /></Td>
                  <Td><span className="text-xs">{formatDate(task.createdAt)}</span></Td>
                  <Td>
                    {isOpen && (
                      <div className="flex items-center gap-1 justify-end">
                        {!task.assignee ? (
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => claimMut.mutate({ id: task.id, userId: user?.email ?? 'admin' })}
                          >
                            Claim
                          </Button>
                        ) : task.assignee === user?.email ? (
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => unclaimMut.mutate(task.id)}
                          >
                            Unclaim
                          </Button>
                        ) : null}
                        <Button
                          size="sm"
                          onClick={() => { setCompleteTask_(task); setCompleteVars('{}'); setCompleteError(''); setFormValues({}); }}
                        >
                          Complete
                        </Button>
                      </div>
                    )}
                  </Td>
                </Tr>
                );
              })}
            </tbody>
          </Table>
        )}
      </Card>

      {/* Complete task modal */}
      <Modal open={!!completeTask_} onClose={() => setCompleteTask_(null)} title="Complete Task">
        <div className="space-y-4">
          {completeTask_?.formKey && formLoading ? (
            <div className="flex justify-center py-6"><Spinner className="w-5 h-5 text-blue-500" /></div>
          ) : usingDynamicForm ? (
            <div className="space-y-3">
              {form!.schema.components!.map(component => (
                <div key={component.key}>
                  <label className="block text-xs font-medium text-gray-700 mb-1.5">
                    {component.label ?? component.key}
                  </label>
                  {component.type === 'checkbox' ? (
                    <input
                      type="checkbox"
                      checked={!!formValues[component.key]}
                      onChange={e => setFormValues(v => ({ ...v, [component.key]: e.target.checked }))}
                      className="h-4 w-4 rounded border-gray-300 focus:ring-2 focus:ring-blue-500"
                    />
                  ) : (
                    <input
                      type="text"
                      value={(formValues[component.key] as string) ?? ''}
                      onChange={e => setFormValues(v => ({ ...v, [component.key]: e.target.value }))}
                      className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  )}
                </div>
              ))}
            </div>
          ) : (
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1.5">
                Output Variables (JSON)
              </label>
              <textarea
                value={completeVars}
                onChange={e => setCompleteVars(e.target.value)}
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono h-28 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              {completeError && <p className="text-xs text-red-500 mt-1">{completeError}</p>}
            </div>
          )}
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setCompleteTask_(null)}>Cancel</Button>
            <Button onClick={handleComplete} disabled={completeMut.isPending}>
              {completeMut.isPending && <Spinner className="w-4 h-4 text-white" />}
              Complete Task
            </Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
