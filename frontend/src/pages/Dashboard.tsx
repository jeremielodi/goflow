import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { Play, CheckCircle, AlertTriangle, GitBranch, Clock, TrendingUp } from 'lucide-react';
import { listDefinitions, listInstances } from '../api/processes';
import { listHistoricInstances } from '../api/history';
import { listIncidents } from '../api/incidents';
import { listTasks } from '../api/tasks';
import { Card, CardHeader, Badge, Spinner, Tr, Td, Table, Thead, Th } from '../components/ui';
import { formatDuration, formatDate } from '../utils/format';

export default function Dashboard() {
  const nav = useNavigate();

  const { data: running = [] }    = useQuery({ queryKey: ['instances', 'running'],    queryFn: () => listInstances({ maxResults: 100 }) });
  const { data: defs = [] }       = useQuery({ queryKey: ['definitions'],             queryFn: () => listDefinitions() });
  const { data: history = [] }    = useQuery({ queryKey: ['history', 'recent'],       queryFn: () => listHistoricInstances({ maxResults: 20 }) });
  const { data: incidents = [] }  = useQuery({ queryKey: ['incidents'],               queryFn: () => listIncidents() });
  const { data: tasks = [] }      = useQuery({ queryKey: ['tasks'],                   queryFn: () => listTasks({ maxResults: 50 }) });

  const completed = history.filter(h => h.state === 'completed').length;

  const kpis = [
    { label: 'Running',    value: running.length,   icon: Play,         color: 'text-blue-500',  bg: 'bg-blue-50'  },
    { label: 'Completed',  value: completed,          icon: CheckCircle,  color: 'text-green-500', bg: 'bg-green-50' },
    { label: 'Incidents',  value: incidents.length,   icon: AlertTriangle,color: 'text-red-500',   bg: 'bg-red-50'   },
    { label: 'Open Tasks', value: tasks.length,        icon: Clock,        color: 'text-amber-500', bg: 'bg-amber-50' },
    { label: 'Definitions',value: defs.length,         icon: GitBranch,    color: 'text-purple-500',bg: 'bg-purple-50'},
  ];

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-xl font-bold text-gray-900">Dashboard</h1>
        <p className="text-sm text-gray-500 mt-0.5">Overview of your workflow engine</p>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        {kpis.map(({ label, value, icon: Icon, color, bg }) => (
          <Card key={label} className="p-4">
            <div className={`w-9 h-9 rounded-lg ${bg} flex items-center justify-center mb-3`}>
              <Icon className={`w-5 h-5 ${color}`} />
            </div>
            <p className="text-2xl font-bold text-gray-900">{value}</p>
            <p className="text-xs text-gray-500 mt-0.5">{label}</p>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Running instances */}
        <Card>
          <CardHeader
            title="Running Instances"
            subtitle={`${running.length} active`}
            actions={
              <button onClick={() => nav('/instances')} className="text-xs text-blue-600 hover:underline">
                View all →
              </button>
            }
          />
          {running.length === 0 ? (
            <p className="text-sm text-gray-400 px-6 py-8 text-center">No running instances</p>
          ) : (
            <Table>
              <Thead>
                <tr><Th>Process</Th><Th>Started</Th><Th>Status</Th></tr>
              </Thead>
              <tbody>
                {running.slice(0, 8).map(inst => (
                  <Tr key={inst.id} onClick={() => nav(`/instances/${inst.id}`)}>
                    <Td>
                      <span className="font-medium text-gray-900 text-xs">{inst.processDefinitionKey}</span>
                      <br />
                      <span className="text-gray-400 text-xs">{inst.id.slice(0, 8)}…</span>
                    </Td>
                    <Td><span className="text-xs">{formatDate(inst.startTime)}</span></Td>
                    <Td><Badge value={inst.status} /></Td>
                  </Tr>
                ))}
              </tbody>
            </Table>
          )}
        </Card>

        {/* Recent history */}
        <Card>
          <CardHeader
            title="Recent Executions"
            subtitle="Last 20 process instances"
            actions={
              <button onClick={() => nav('/history')} className="text-xs text-blue-600 hover:underline">
                View all →
              </button>
            }
          />
          {history.length === 0 ? (
            <p className="text-sm text-gray-400 px-6 py-8 text-center">No history yet</p>
          ) : (
            <Table>
              <Thead>
                <tr><Th>Process</Th><Th>Duration</Th><Th>State</Th></tr>
              </Thead>
              <tbody>
                {history.slice(0, 8).map(inst => (
                  <Tr key={inst.id} onClick={() => nav(`/instances/${inst.id}`)}>
                    <Td>
                      <span className="font-medium text-gray-900 text-xs">{inst.processDefinitionKey}</span>
                      <br />
                      <span className="text-gray-400 text-xs">{inst.id.slice(0, 8)}…</span>
                    </Td>
                    <Td>
                      <span className="text-xs">{inst.durationInMillis != null ? formatDuration(inst.durationInMillis) : '—'}</span>
                    </Td>
                    <Td><Badge value={inst.state} /></Td>
                  </Tr>
                ))}
              </tbody>
            </Table>
          )}
        </Card>
      </div>

      {/* Open tasks */}
      {tasks.length > 0 && (
        <Card>
          <CardHeader
            title="Open User Tasks"
            subtitle={`${tasks.length} tasks awaiting action`}
            actions={
              <button onClick={() => nav('/tasks')} className="text-xs text-blue-600 hover:underline">
                View all →
              </button>
            }
          />
          <Table>
            <Thead>
              <tr><Th>Task</Th><Th>Process Instance</Th><Th>Assignee</Th><Th>Created</Th></tr>
            </Thead>
            <tbody>
              {tasks.slice(0, 5).map(task => (
                <Tr key={task.id} onClick={() => nav(`/tasks?instance=${task.processInstanceId}`)}>
                  <Td><span className="font-medium text-xs">{task.name}</span></Td>
                  <Td><span className="text-xs text-gray-500">{task.processInstanceId?.slice(0, 8)}…</span></Td>
                  <Td><span className="text-xs">{task.assignee ?? <span className="text-gray-400">Unassigned</span>}</span></Td>
                  <Td><span className="text-xs">{formatDate(task.created)}</span></Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </Card>
      )}

      {/* Incidents warning */}
      {incidents.length > 0 && (
        <Card className="border-red-200">
          <CardHeader
            title={`⚠ ${incidents.length} Open Incident${incidents.length > 1 ? 's' : ''}`}
            subtitle="Processes with failed jobs"
            actions={
              <button onClick={() => nav('/incidents')} className="text-xs text-red-600 hover:underline">
                Resolve →
              </button>
            }
          />
        </Card>
      )}
    </div>
  );
}
