import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { History as HistoryIcon, RefreshCw } from 'lucide-react';
import { listHistoricInstances } from '../api/history';
import { listDefinitions } from '../api/processes';
import { Card, Table, Thead, Th, Tr, Td, Badge, Button, Spinner, EmptyState } from '../components/ui';
import { formatDate, formatDuration } from '../utils/format';

export default function History() {
  const nav = useNavigate();
  const [keyFilter, setKeyFilter] = useState('');
  const [stateFilter, setStateFilter] = useState('');

  const { data: history = [], isLoading, refetch } = useQuery({
    queryKey: ['history', keyFilter, stateFilter],
    queryFn: () => listHistoricInstances({
      processDefinitionKey: keyFilter || undefined,
      state: stateFilter || undefined,
      maxResults: 200,
    }),
    refetchInterval: 10000,
  });

  const { data: defs = [] } = useQuery({
    queryKey: ['definitions'],
    queryFn: () => listDefinitions(),
  });

  const defKeys = [...new Set(defs.map(d => d.key))].sort();

  const counts = history.reduce<Record<string, number>>((acc, h) => {
    acc[h.state] = (acc[h.state] ?? 0) + 1;
    return acc;
  }, {});

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">History</h1>
          <p className="text-sm text-gray-500 mt-0.5">{history.length} process instances</p>
        </div>
        <Button variant="ghost" size="sm" onClick={() => refetch()}>
          <RefreshCw className="w-4 h-4" />
        </Button>
      </div>

      {/* State summary */}
      <div className="flex flex-wrap gap-3">
        {[
          { state: '',           label: 'All',        count: history.length },
          { state: 'active',     label: 'Active',     count: counts.active ?? 0 },
          { state: 'completed',  label: 'Completed',  count: counts.completed ?? 0 },
          { state: 'terminated', label: 'Terminated', count: counts.terminated ?? 0 },
          { state: 'suspended',  label: 'Suspended',  count: counts.suspended ?? 0 },
        ].map(({ state, label, count }) => (
          <button
            key={state}
            onClick={() => setStateFilter(state)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
              stateFilter === state
                ? 'bg-blue-600 text-white'
                : 'bg-white border border-gray-300 text-gray-700 hover:bg-gray-50'
            }`}
          >
            {label} ({count})
          </button>
        ))}
      </div>

      {/* Filters */}
      <Card className="p-4">
        <div className="flex flex-wrap gap-3">
          <select
            value={keyFilter}
            onChange={e => setKeyFilter(e.target.value)}
            className="text-sm border border-gray-300 rounded-md px-3 py-1.5 focus:outline-none focus:ring-2 focus:ring-blue-500 flex-1 min-w-[180px]"
          >
            <option value="">All process keys</option>
            {defKeys.map(k => <option key={k} value={k}>{k}</option>)}
          </select>
          <Button variant="secondary" size="sm" onClick={() => { setKeyFilter(''); setStateFilter(''); }}>
            Clear
          </Button>
        </div>
      </Card>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner className="w-6 h-6 text-blue-500" /></div>
        ) : history.length === 0 ? (
          <EmptyState
            icon={<HistoryIcon className="w-12 h-12" />}
            title="No history records"
            description="Start a process to see it here"
          />
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>Instance ID</Th>
                <Th>Process Key</Th>
                <Th>Started</Th>
                <Th>Ended</Th>
                <Th>Duration</Th>
                <Th>State</Th>
              </tr>
            </Thead>
            <tbody>
              {history.map(inst => (
                <Tr key={inst.id} onClick={() => nav(`/instances/${inst.id}`)}>
                  <Td>
                    <span className="font-mono text-xs text-gray-500">{inst.id.slice(0, 8)}…</span>
                  </Td>
                  <Td>
                    <span className="font-medium text-sm">{inst.processDefinitionKey}</span>
                  </Td>
                  <Td><span className="text-xs">{formatDate(inst.startTime)}</span></Td>
                  <Td><span className="text-xs">{inst.endTime ? formatDate(inst.endTime) : '—'}</span></Td>
                  <Td>
                    <span className="text-xs">
                      {inst.durationInMillis != null ? formatDuration(inst.durationInMillis) : '—'}
                    </span>
                  </Td>
                  <Td><Badge value={inst.state} /></Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        )}
      </Card>
    </div>
  );
}
