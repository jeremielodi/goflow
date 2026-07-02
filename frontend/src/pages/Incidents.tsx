import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { AlertTriangle, RefreshCw, RotateCcw, Trash2 } from 'lucide-react';
import { listIncidents, deleteIncident, retryJob } from '../api/incidents';
import { Card, Table, Thead, Th, Tr, Td, Button, Spinner, EmptyState } from '../components/ui';
import { formatDate } from '../utils/format';

export default function Incidents() {
  const qc = useQueryClient();
  const nav = useNavigate();

  const { data: incidents = [], isLoading, refetch } = useQuery({
    queryKey: ['incidents'],
    queryFn: () => listIncidents(),
    refetchInterval: 10000,
  });

  const retryMut = useMutation({
    mutationFn: (jobId: string) => retryJob(jobId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['incidents'] }),
  });

  const deleteMut = useMutation({
    mutationFn: deleteIncident,
    onSuccess: () => qc.invalidateQueries({ queryKey: ['incidents'] }),
  });

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Incidents</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {incidents.length > 0
              ? `${incidents.length} open incident${incidents.length > 1 ? 's' : ''}`
              : 'All clear'}
          </p>
        </div>
        <Button variant="ghost" size="sm" onClick={() => refetch()}>
          <RefreshCw className="w-4 h-4" />
        </Button>
      </div>

      {incidents.length > 0 && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 flex items-start gap-3">
          <AlertTriangle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
          <div>
            <p className="text-sm font-medium text-red-800">
              {incidents.length} process{incidents.length > 1 ? 'es' : ''} require attention
            </p>
            <p className="text-xs text-red-600 mt-0.5">
              Use "Retry" to re-queue the failed job with fresh retries.
            </p>
          </div>
        </div>
      )}

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner className="w-6 h-6 text-blue-500" /></div>
        ) : incidents.length === 0 ? (
          <EmptyState
            icon={<AlertTriangle className="w-12 h-12" />}
            title="No incidents"
            description="All processes are running normally"
          />
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>Type</Th>
                <Th>Activity</Th>
                <Th>Process Instance</Th>
                <Th>Message</Th>
                <Th>Time</Th>
                <Th></Th>
              </tr>
            </Thead>
            <tbody>
              {incidents.map(inc => (
                <Tr key={inc.id}>
                  <Td>
                    <span className="inline-flex items-center gap-1 text-xs text-red-700 bg-red-50 px-2 py-0.5 rounded-full">
                      <AlertTriangle className="w-3 h-3" />
                      {inc.incidentType}
                    </span>
                  </Td>
                  <Td><span className="text-xs font-mono">{inc.activityId}</span></Td>
                  <Td>
                    <button
                      onClick={() => nav(`/instances/${inc.processInstanceId}`)}
                      className="text-xs text-blue-600 hover:underline font-mono"
                    >
                      {inc.processInstanceId?.slice(0, 8)}…
                    </button>
                  </Td>
                  <Td>
                    <span className="text-xs text-gray-600 max-w-xs truncate block" title={inc.errorMessage}>
                      {inc.errorMessage ?? '—'}
                    </span>
                  </Td>
                  <Td><span className="text-xs">{formatDate(inc.createdAt)}</span></Td>
                  <Td>
                    <div className="flex items-center gap-1 justify-end">
                      <Button
                        size="sm"
                        variant="secondary"
                        onClick={() => retryMut.mutate(inc.jobId ?? inc.id)}
                        disabled={retryMut.isPending}
                        title="Retry job (3 retries)"
                      >
                        <RotateCcw className="w-3.5 h-3.5" /> Retry
                      </Button>
                      <Button
                        size="sm"
                        variant="danger"
                        onClick={() => {
                          if (confirm('Delete this incident?')) deleteMut.mutate(inc.id);
                        }}
                        disabled={deleteMut.isPending}
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </Button>
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
