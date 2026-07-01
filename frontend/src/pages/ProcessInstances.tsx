import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { RefreshCw, Search, Trash2, PauseCircle, PlayCircle } from 'lucide-react';
import { listInstances, terminateInstance, suspendInstance } from '../api/processes';
import { listDefinitions } from '../api/processes';
import { Card, Table, Thead, Th, Tr, Td, Badge, Button, Input, Spinner, EmptyState } from '../components/ui';
import { formatDate } from '../utils/format';

export default function ProcessInstances() {
  const qc = useQueryClient();
  const nav = useNavigate();
  const [searchParams] = useSearchParams();

  const [keyFilter, setKeyFilter]    = useState(searchParams.get('key') ?? '');
  const [statusFilter, setStatus]    = useState('');

  const { data: instances = [], isLoading, refetch } = useQuery({
    queryKey: ['instances', keyFilter, statusFilter],
    queryFn: () => listInstances({
      processDefinitionKey: keyFilter || undefined,
      status: statusFilter || undefined,
      maxResults: 200,
    }),
    refetchInterval: 5000,
  });

  const { data: defs = [] } = useQuery({
    queryKey: ['definitions'],
    queryFn: () => listDefinitions(),
  });

  const terminateMut = useMutation({
    mutationFn: terminateInstance,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['instances'] }),
  });

  const suspendMut = useMutation({
    mutationFn: ({ id, suspended }: { id: string; suspended: boolean }) =>
      suspendInstance(id, suspended),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['instances'] }),
  });

  const defKeys = [...new Set(defs.map(d => d.key))].sort();

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Process Instances</h1>
          <p className="text-sm text-gray-500 mt-0.5">{instances.length} instances</p>
        </div>
        <Button variant="ghost" size="sm" onClick={() => refetch()}>
          <RefreshCw className="w-4 h-4" />
        </Button>
      </div>

      {/* Filters */}
      <Card className="p-4">
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2 flex-1 min-w-[200px]">
            <Search className="w-4 h-4 text-gray-400 flex-shrink-0" />
            <select
              value={keyFilter}
              onChange={e => setKeyFilter(e.target.value)}
              className="flex-1 text-sm border border-gray-300 rounded-md px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">All process keys</option>
              {defKeys.map(k => <option key={k} value={k}>{k}</option>)}
            </select>
          </div>
          <select
            value={statusFilter}
            onChange={e => setStatus(e.target.value)}
            className="text-sm border border-gray-300 rounded-md px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All statuses</option>
            <option value="running">Running</option>
            <option value="suspended">Suspended</option>
            <option value="completed">Completed</option>
            <option value="terminated">Terminated</option>
          </select>
          <Button variant="secondary" size="sm" onClick={() => { setKeyFilter(''); setStatus(''); }}>
            Clear
          </Button>
        </div>
      </Card>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner className="w-6 h-6 text-blue-500" /></div>
        ) : instances.length === 0 ? (
          <EmptyState title="No instances found" description="Start a process or adjust filters" />
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>ID</Th>
                <Th>Process Key</Th>
                <Th>Started</Th>
                <Th>Status</Th>
                <Th></Th>
              </tr>
            </Thead>
            <tbody>
              {instances.map(inst => (
                <Tr key={inst.id} onClick={() => nav(`/instances/${inst.id}`)}>
                  <Td>
                    <span className="font-mono text-xs text-gray-500">{inst.id.slice(0, 8)}…</span>
                  </Td>
                  <Td>
                    <span className="font-medium text-sm">{inst.processDefinitionKey}</span>
                  </Td>
                  <Td><span className="text-xs">{formatDate(inst.startTime)}</span></Td>
                  <Td><Badge value={inst.status} /></Td>
                  <Td>
                    <div className="flex items-center gap-1 justify-end" onClick={e => e.stopPropagation()}>
                      {inst.status === 'running' && (
                        <button
                          onClick={() => suspendMut.mutate({ id: inst.id, suspended: true })}
                          className="text-gray-400 hover:text-amber-500 p-1"
                          title="Suspend"
                        >
                          <PauseCircle className="w-4 h-4" />
                        </button>
                      )}
                      {inst.status === 'suspended' && (
                        <button
                          onClick={() => suspendMut.mutate({ id: inst.id, suspended: false })}
                          className="text-gray-400 hover:text-green-500 p-1"
                          title="Resume"
                        >
                          <PlayCircle className="w-4 h-4" />
                        </button>
                      )}
                      {(inst.status === 'running' || inst.status === 'suspended') && (
                        <button
                          onClick={() => {
                            if (confirm('Terminate this instance?')) terminateMut.mutate(inst.id);
                          }}
                          className="text-gray-400 hover:text-red-500 p-1"
                          title="Terminate"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      )}
                    </div>
                  </Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        )}
      </Card>
    </div>
  );
}
