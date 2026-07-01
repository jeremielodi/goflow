import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { ArrowLeft, RefreshCw, ChevronDown, ChevronUp, Info } from 'lucide-react';
import { getInstance, getInstanceVariables, getInstanceState } from '../api/processes';
import { listHistoricActivities } from '../api/history';
import { listTasks } from '../api/tasks';
import { apiClient } from '../api/client';
import { BpmnViewer } from '../components/bpmn/BpmnViewer';
import { Card, CardHeader, Badge, Button, Spinner, Table, Thead, Th, Tr, Td } from '../components/ui';
import { formatDate, formatDuration } from '../utils/format';
import type { ProcessDefinition } from '../types';

export default function InstanceDetail() {
  const { id } = useParams<{ id: string }>();
  const nav = useNavigate();
  const [showVars, setShowVars] = useState(false);

  const { data: inst, isLoading: instLoading, refetch } = useQuery({
    queryKey: ['instance', id],
    queryFn: () => getInstance(id!),
    refetchInterval: 3000,
  });

  const { data: def } = useQuery({
    queryKey: ['definition', inst?.processDefinitionId],
    queryFn: () => apiClient.get<ProcessDefinition>(`/engine-rest/process-definition/${inst!.processDefinitionId}`).then(r => r.data),
    enabled: !!inst?.processDefinitionId,
  });

  // Raw BPMN XML for the viewer
  const { data: bpmnXml } = useQuery({
    queryKey: ['bpmn-xml', inst?.processDefinitionId],
    queryFn: () =>
      apiClient.get<string>(`/engine-rest/process-definition/${inst!.processDefinitionId}`)
        .then(r => typeof r.data === 'string' ? r.data : (r.data as { bpmnXml: string }).bpmnXml),
    enabled: !!inst?.processDefinitionId,
  });

  const { data: activities = [] } = useQuery({
    queryKey: ['activities', id],
    queryFn: () => listHistoricActivities(id!),
    enabled: !!id,
    refetchInterval: 3000,
  });

  const { data: variables } = useQuery({
    queryKey: ['variables', id],
    queryFn: () => getInstanceVariables(id!),
    enabled: !!id,
  });

  const { data: tasks = [] } = useQuery({
    queryKey: ['tasks', id],
    queryFn: () => listTasks({ processInstanceId: id! }),
    enabled: !!id,
    refetchInterval: 3000,
  });

  if (instLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Spinner className="w-8 h-8 text-blue-500" />
      </div>
    );
  }

  if (!inst) {
    return (
      <div className="p-6">
        <p className="text-gray-500">Instance not found.</p>
        <Button variant="ghost" size="sm" onClick={() => nav('/instances')} className="mt-2">
          ← Back
        </Button>
      </div>
    );
  }

  const activeActivities = activities.filter(a => !a.endTime && !a.canceled);

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <button onClick={() => nav('/instances')} className="text-gray-400 hover:text-gray-600">
            <ArrowLeft className="w-5 h-5" />
          </button>
          <div>
            <h1 className="text-xl font-bold text-gray-900">
              {def?.name ?? inst.processDefinitionKey}
            </h1>
            <p className="text-xs text-gray-400 font-mono mt-0.5">{inst.id}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Badge value={inst.status} />
          <Button variant="ghost" size="sm" onClick={() => refetch()}>
            <RefreshCw className="w-4 h-4" />
          </Button>
        </div>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Started',   value: formatDate(inst.startTime) },
          { label: 'Duration',  value: inst.durationInMillis != null ? formatDuration(inst.durationInMillis) : '—' },
          { label: 'Active tasks', value: activeActivities.length },
          { label: 'Definition', value: `v${def?.version ?? '?'}` },
        ].map(({ label, value }) => (
          <Card key={label} className="p-4">
            <p className="text-xs text-gray-500">{label}</p>
            <p className="text-sm font-semibold text-gray-900 mt-1">{value}</p>
          </Card>
        ))}
      </div>

      {/* BPMN Viewer */}
      {bpmnXml ? (
        <Card>
          <CardHeader
            title="Process Diagram"
            subtitle={`${activeActivities.length} active token${activeActivities.length !== 1 ? 's' : ''}`}
          />
          <BpmnViewer xml={bpmnXml} activities={activities} className="h-96" />
          {/* Legend */}
          <div className="flex items-center gap-4 px-6 py-3 border-t border-gray-100 text-xs text-gray-500">
            <span className="flex items-center gap-1.5">
              <span className="w-3 h-3 rounded-full bg-blue-500 inline-block" /> Active
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-3 h-3 rounded-full bg-green-500 inline-block" /> Completed
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-3 h-3 rounded-full bg-amber-500 inline-block" /> Canceled
            </span>
          </div>
        </Card>
      ) : (
        <Card className="p-6 text-center text-gray-400 text-sm">
          <Info className="w-8 h-8 mx-auto mb-2 text-gray-300" />
          No BPMN diagram available
        </Card>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Activity log */}
        <Card>
          <CardHeader title="Activity History" subtitle={`${activities.length} elements executed`} />
          {activities.length === 0 ? (
            <p className="text-sm text-gray-400 p-6">No activities yet</p>
          ) : (
            <Table>
              <Thead>
                <tr><Th>Element</Th><Th>Type</Th><Th>Duration</Th><Th>State</Th></tr>
              </Thead>
              <tbody>
                {activities.map(a => (
                  <Tr key={a.id}>
                    <Td>
                      <p className="font-medium text-xs">{a.activityName ?? a.activityId}</p>
                      <p className="text-gray-400 text-xs">{a.activityId}</p>
                    </Td>
                    <Td><span className="text-xs">{a.activityType}</span></Td>
                    <Td>
                      <span className="text-xs">
                        {a.durationInMillis != null ? formatDuration(a.durationInMillis) : '—'}
                      </span>
                    </Td>
                    <Td>
                      <Badge
                        value={a.canceled ? 'canceled' : a.endTime ? 'completed' : 'active'}
                      />
                    </Td>
                  </Tr>
                ))}
              </tbody>
            </Table>
          )}
        </Card>

        {/* Variables */}
        <Card>
          <CardHeader
            title="Process Variables"
            subtitle={variables ? `${Object.keys(variables).length} variables` : ''}
            actions={
              <button
                onClick={() => setShowVars(v => !v)}
                className="text-gray-400 hover:text-gray-600"
              >
                {showVars ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
              </button>
            }
          />
          {showVars && variables && (
            <Table>
              <Thead>
                <tr><Th>Name</Th><Th>Type</Th><Th>Value</Th></tr>
              </Thead>
              <tbody>
                {Object.entries(variables).map(([name, v]) => (
                  <Tr key={name}>
                    <Td><span className="font-mono text-xs font-medium">{name}</span></Td>
                    <Td><span className="text-xs text-gray-500">{v.type}</span></Td>
                    <Td>
                      <span className="font-mono text-xs break-all">
                        {String(v.value)}
                      </span>
                    </Td>
                  </Tr>
                ))}
              </tbody>
            </Table>
          )}
          {!showVars && (
            <p className="text-xs text-gray-400 px-6 py-4 cursor-pointer" onClick={() => setShowVars(true)}>
              Click to expand…
            </p>
          )}
        </Card>
      </div>

      {/* Pending user tasks */}
      {tasks.length > 0 && (
        <Card>
          <CardHeader title={`Open User Tasks (${tasks.length})`} />
          <Table>
            <Thead>
              <tr><Th>Task</Th><Th>Assignee</Th><Th>Created</Th></tr>
            </Thead>
            <tbody>
              {tasks.map(task => (
                <Tr key={task.id} onClick={() => nav(`/tasks?instance=${task.processInstanceId}`)}>
                  <Td><span className="font-medium text-sm">{task.name}</span></Td>
                  <Td><span className="text-xs">{task.assignee ?? <span className="text-gray-400">Unassigned</span>}</span></Td>
                  <Td><span className="text-xs">{formatDate(task.created)}</span></Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </Card>
      )}
    </div>
  );
}
