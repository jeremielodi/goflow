import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { ArrowLeft, RefreshCw, ChevronDown, ChevronUp, Info, Move, Play, Pause, SkipBack, SkipForward, X } from 'lucide-react';
import { getInstance, getInstanceVariables, getInstanceState, modifyInstance } from '../api/processes';
import { listHistoricActivities } from '../api/history';
import { getTokenHistory } from '../api/audit';
import { listTasks } from '../api/tasks';
import { apiClient } from '../api/client';
import { BpmnViewer } from '../components/bpmn/BpmnViewer';
import { Card, CardHeader, Badge, Button, Spinner, Table, Thead, Th, Tr, Td, Modal } from '../components/ui';
import { formatDate, formatDuration } from '../utils/format';
import type { ProcessDefinition } from '../types';

export default function InstanceDetail() {
  const { id } = useParams<{ id: string }>();
  const nav = useNavigate();
  const qc = useQueryClient();
  const [showVars, setShowVars] = useState(false);
  const [showModify, setShowModify] = useState(false);
  const [sourceElementId, setSourceElementId] = useState('');
  const [targetElementId, setTargetElementId] = useState('');
  const [modifyError, setModifyError] = useState('');

  // Replay: step through the audit-log-derived token history and highlight
  // each position on the diagram. Off by default so the diagram's default
  // appearance (live state only) is unchanged until the user opts in.
  const [replayEngaged, setReplayEngaged] = useState(false);
  const [replayIndex, setReplayIndex] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);

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

  // Sourced from audit_logs (not the live executions table), so it reflects
  // the full path a token took over time rather than just its current
  // position — see internal/service/token_history_service.go.
  const { data: tokenHistory = [] } = useQuery({
    queryKey: ['token-history', id],
    queryFn: () => getTokenHistory(id!),
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

  const modifyMut = useMutation({
    mutationFn: () => modifyInstance(id!, sourceElementId, targetElementId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['activities', id] });
      qc.invalidateQueries({ queryKey: ['tasks', id] });
      qc.invalidateQueries({ queryKey: ['instance', id] });
      setShowModify(false);
    },
    onError: (e: any) => {
      setModifyError(e.response?.data?.detail ?? e.message ?? 'Failed to move token');
    },
  });

  const handleModify = () => {
    setModifyError('');
    if (!sourceElementId || !targetElementId) {
      setModifyError('Pick a source token and enter a target element ID');
      return;
    }
    modifyMut.mutate();
  };

  // Only steps that land on a concrete BPMN element make sense to scrub
  // through on the diagram (e.g. PROCESS_STARTED has no elementId).
  const replaySteps = tokenHistory.filter(s => s.elementId);

  // Reset replay state when navigating to a different instance.
  useEffect(() => {
    setReplayEngaged(false);
    setIsPlaying(false);
    setReplayIndex(0);
  }, [id]);

  useEffect(() => {
    if (!isPlaying) return;
    if (replaySteps.length === 0) {
      setIsPlaying(false);
      return;
    }
    const timer = setInterval(() => {
      setReplayIndex(i => {
        if (i >= replaySteps.length - 1) {
          setIsPlaying(false);
          return i;
        }
        return i + 1;
      });
    }, 1200);
    return () => clearInterval(timer);
  }, [isPlaying, replaySteps.length]);

  const startReplay = () => {
    setReplayEngaged(true);
    setReplayIndex(0);
    setIsPlaying(true);
  };
  const exitReplay = () => {
    setReplayEngaged(false);
    setIsPlaying(false);
  };
  const stepReplay = (i: number) => {
    setIsPlaying(false);
    setReplayIndex(Math.max(0, Math.min(replaySteps.length - 1, i)));
  };

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
  const durationMs = inst.endedAt
    ? new Date(inst.endedAt).getTime() - new Date(inst.startedAt).getTime()
    : null;

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
              {def?.name ?? inst.processKey}
            </h1>
            <p className="text-xs text-gray-400 font-mono mt-0.5">{inst.id}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Badge value={inst.status} />
          {inst.status === 'running' && activeActivities.length > 0 && (
            <Button
              variant="secondary"
              size="sm"
              onClick={() => {
                setSourceElementId(activeActivities[0].activityId);
                setTargetElementId('');
                setModifyError('');
                setShowModify(true);
              }}
            >
              <Move className="w-4 h-4" /> Move Token
            </Button>
          )}
          <Button variant="ghost" size="sm" onClick={() => refetch()}>
            <RefreshCw className="w-4 h-4" />
          </Button>
        </div>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Started',   value: formatDate(inst.startedAt) },
          { label: 'Duration',  value: durationMs != null ? formatDuration(durationMs) : '—' },
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
            actions={
              replaySteps.length > 0 ? (
                replayEngaged ? (
                  <Button variant="ghost" size="sm" onClick={exitReplay}>
                    <X className="w-4 h-4" /> Exit Replay
                  </Button>
                ) : (
                  <Button variant="secondary" size="sm" onClick={startReplay}>
                    <Play className="w-4 h-4" /> Replay
                  </Button>
                )
              ) : undefined
            }
          />
          <BpmnViewer
            xml={bpmnXml}
            activities={activities}
            replayElementId={replayEngaged ? replaySteps[replayIndex]?.elementId : undefined}
            className="h-96"
          />

          {/* Replay controls */}
          {replayEngaged && replaySteps.length > 0 && (
            <div className="px-6 py-3 border-t border-gray-100 space-y-2">
              <div className="flex items-center gap-3">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => stepReplay(replayIndex - 1)}
                  disabled={replayIndex === 0}
                >
                  <SkipBack className="w-4 h-4" />
                </Button>
                <Button variant="ghost" size="sm" onClick={() => setIsPlaying(p => !p)}>
                  {isPlaying ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => stepReplay(replayIndex + 1)}
                  disabled={replayIndex === replaySteps.length - 1}
                >
                  <SkipForward className="w-4 h-4" />
                </Button>
                <input
                  type="range"
                  min={0}
                  max={replaySteps.length - 1}
                  value={replayIndex}
                  onChange={e => stepReplay(Number(e.target.value))}
                  className="flex-1"
                />
                <span className="text-xs text-gray-400 whitespace-nowrap font-mono">
                  {replayIndex + 1} / {replaySteps.length}
                </span>
              </div>
              <p className="text-xs text-gray-500">
                <span className="font-medium text-gray-700">
                  {replaySteps[replayIndex]?.elementName ?? replaySteps[replayIndex]?.elementId}
                </span>
                {replaySteps[replayIndex]?.detail && <> — {replaySteps[replayIndex]?.detail}</>}
                {' · '}
                {formatDate(replaySteps[replayIndex]?.timestamp)}
              </p>
            </div>
          )}

          {/* Legend */}
          <div className="flex items-center gap-4 px-6 py-3 border-t border-gray-100 text-xs text-gray-500">
            <span className="flex items-center gap-1.5">
              <span className="w-3 h-3 rounded-full bg-blue-500 inline-block" /> Active
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-3 h-3 rounded-full bg-green-500 inline-block" /> Completed
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-3 h-3 rounded-full bg-amber-500 inline-block" /> Terminated
            </span>
            {replayEngaged && (
              <span className="flex items-center gap-1.5">
                <span className="w-3 h-3 rounded-full bg-violet-500 inline-block" /> Replay position
              </span>
            )}
          </div>
        </Card>
      ) : (
        <Card className="p-6 text-center text-gray-400 text-sm">
          <Info className="w-8 h-8 mx-auto mb-2 text-gray-300" />
          No BPMN diagram available
        </Card>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Activity log — the token's full path over time, derived from audit_logs */}
        <Card>
          <CardHeader title="Activity History" subtitle={`${tokenHistory.length} recorded events`} />
          {tokenHistory.length === 0 ? (
            <p className="text-sm text-gray-400 p-6">No activity yet</p>
          ) : (
            <ul className="divide-y divide-gray-100 max-h-96 overflow-y-auto">
              {tokenHistory.map((step, i) => (
                <li
                  key={i}
                  onClick={
                    step.elementId
                      ? () => {
                          setReplayEngaged(true);
                          setIsPlaying(false);
                          setReplayIndex(Math.max(0, replaySteps.indexOf(step)));
                        }
                      : undefined
                  }
                  className={`px-6 py-3 flex items-start justify-between gap-3 ${step.elementId ? 'cursor-pointer hover:bg-gray-50' : ''}`}
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <Badge value={step.action} label={step.action.replace(/_/g, ' ')} />
                      {(step.elementName ?? step.elementId) && (
                        <span className="text-xs font-medium text-gray-700">
                          {step.elementName ?? step.elementId}
                        </span>
                      )}
                    </div>
                    {step.detail && <p className="text-xs text-gray-500 mt-1">{step.detail}</p>}
                  </div>
                  <span className="text-xs text-gray-400 whitespace-nowrap">{formatDate(step.timestamp)}</span>
                </li>
              ))}
            </ul>
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
              <tr><Th>Task</Th><Th>Assignee</Th><Th>Status</Th><Th>Created</Th></tr>
            </Thead>
            <tbody>
              {tasks.map(task => (
                <Tr key={task.id} onClick={() => nav(`/tasks?instance=${task.processInstanceId}`)}>
                  <Td><span className="font-medium text-sm">{task.taskName}</span></Td>
                  <Td><span className="text-xs">{task.assignee ?? <span className="text-gray-400">Unassigned</span>}</span></Td>
                  <Td><Badge value={task.status} /></Td>
                  <Td><span className="text-xs">{formatDate(task.createdAt)}</span></Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        </Card>
      )}

      {/* Move token modal */}
      <Modal open={showModify} onClose={() => setShowModify(false)} title="Move Token">
        <div className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1.5">
              From (active token)
            </label>
            <select
              value={sourceElementId}
              onChange={e => setSourceElementId(e.target.value)}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {activeActivities.map(a => (
                <option key={a.id} value={a.activityId}>
                  {a.activityName ?? a.activityId}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1.5">
              To (target element ID)
            </label>
            <input
              type="text"
              value={targetElementId}
              onChange={e => setTargetElementId(e.target.value)}
              placeholder="e.g. approveTask"
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <p className="text-xs text-gray-400 mt-1">
              The BPMN element ID to jump to — see the diagram above.
            </p>
          </div>
          {modifyError && <p className="text-xs text-red-500">{modifyError}</p>}
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setShowModify(false)}>Cancel</Button>
            <Button onClick={handleModify} disabled={modifyMut.isPending}>
              {modifyMut.isPending && <Spinner className="w-4 h-4 text-white" />}
              Move Token
            </Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
