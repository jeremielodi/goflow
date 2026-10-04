import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { ArrowLeft, Play } from 'lucide-react';
import { getDefinition, getActiveTokenCounts, getInstancePositions, listInstances, getInstanceVariables } from '../api/processes';
import { BpmnViewer } from '../components/bpmn/BpmnViewer';
import { Card, CardHeader, Badge, Button, Spinner, Table, Thead, Th, Tr, Td, EmptyState, Modal } from '../components/ui';
import { formatDate } from '../utils/format';

export default function ProcessDefinitionDetail() {
  const { id } = useParams<{ id: string }>();
  const nav = useNavigate();
  const [selectedInstanceId, setSelectedInstanceId] = useState<string | null>(null);

  const { data: def, isLoading } = useQuery({
    queryKey: ['definition', id],
    queryFn: () => getDefinition(id!),
    enabled: !!id,
  });

  // Live snapshot of where currently-running instances of this key are
  // sitting — polled, since it reflects a moving system, not a fetch-once
  // static diagram.
  const { data: tokenCounts = [] } = useQuery({
    queryKey: ['token-counts', def?.key],
    queryFn: () => getActiveTokenCounts(def!.key),
    enabled: !!def?.key,
    refetchInterval: 3000,
  });
  const totalActiveTokens = tokenCounts.reduce((sum, t) => sum + t.count, 0);

  const { data: instances = [], isLoading: instancesLoading } = useQuery({
    queryKey: ['instances', def?.key],
    queryFn: () => listInstances({ processKey: def!.key, maxResults: 50 }),
    enabled: !!def?.key,
    refetchInterval: 5000,
  });

  // Where each running/suspended instance's token(s) currently sit — shown
  // directly in the list so a running instance's state is visible without
  // opening the full replay view.
  const { data: instancePositions = [] } = useQuery({
    queryKey: ['instance-positions', def?.key],
    queryFn: () => getInstancePositions(def!.key),
    enabled: !!def?.key,
    refetchInterval: 3000,
  });
  const positionsByInstance = new Map<string, string[]>();
  for (const p of instancePositions) {
    const list = positionsByInstance.get(p.processInstanceId) ?? [];
    list.push(p.elementId);
    positionsByInstance.set(p.processInstanceId, list);
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Spinner className="w-8 h-8 text-blue-500" />
      </div>
    );
  }

  if (!def) {
    return (
      <div className="p-6">
        <p className="text-gray-500">Process definition not found.</p>
        <Button variant="ghost" size="sm" onClick={() => nav('/processes')} className="mt-2">
          ← Back
        </Button>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-center gap-3">
          <button onClick={() => nav('/processes')} className="text-gray-400 hover:text-gray-600">
            <ArrowLeft className="w-5 h-5" />
          </button>
          <div>
            <h1 className="text-xl font-bold text-gray-900">{def.name || def.key}</h1>
            <p className="text-xs text-gray-400 font-mono mt-0.5">{def.key}</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Badge value="default" label={`v${def.version}`} />
          <Button size="sm" onClick={() => nav(`/instances?key=${def.key}`)}>
            <Play className="w-3.5 h-3.5" /> Instances
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        {[
          { label: 'Deployed', value: formatDate(def.createdAt) },
          { label: 'Deployment ID', value: def.deploymentId },
          { label: 'Tenant', value: def.tenantId || '—' },
        ].map(({ label, value }) => (
          <Card key={label} className="p-4">
            <p className="text-xs text-gray-500">{label}</p>
            <p className="text-sm font-semibold text-gray-900 mt-1 truncate" title={String(value)}>{value}</p>
          </Card>
        ))}
      </div>

      <Card>
        <CardHeader
          title="Process Diagram"
          subtitle={
            totalActiveTokens > 0
              ? `Version ${def.version} · ${totalActiveTokens} active token${totalActiveTokens !== 1 ? 's' : ''} across running instances`
              : `Version ${def.version}`
          }
        />
        {def.bpmnXml ? (
          <BpmnViewer xml={def.bpmnXml} tokenCounts={tokenCounts} className="h-[32rem]" />
        ) : (
          <p className="text-sm text-gray-400 p-6">No BPMN XML available for this definition.</p>
        )}
      </Card>

      <Card>
        <CardHeader title="Process Instances" subtitle={`${instances.length} instance${instances.length !== 1 ? 's' : ''}`} />
        {instancesLoading ? (
          <div className="flex justify-center py-12"><Spinner className="w-6 h-6 text-blue-500" /></div>
        ) : instances.length === 0 ? (
          <EmptyState title="No instances" description="Start this process to see instances here" />
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>ID</Th>
                <Th>Current Node</Th>
                <Th>Started</Th>
                <Th>Ended</Th>
                <Th>Status</Th>
              </tr>
            </Thead>
            <tbody>
              {instances.map(inst => {
                const positions = positionsByInstance.get(inst.id) ?? [];
                return (
                  <Tr key={inst.id} onClick={() => setSelectedInstanceId(inst.id)}>
                    <Td><span className="font-mono text-xs text-gray-500">{inst.id.slice(0, 8)}…</span></Td>
                    <Td>
                      {positions.length > 0 ? (
                        <div className="flex flex-wrap gap-1">
                          {positions.map(elementId => (
                            <span key={elementId} className="font-mono text-[11px] bg-cyan-50 text-cyan-700 border border-cyan-200 rounded px-1.5 py-0.5">
                              {elementId}
                            </span>
                          ))}
                        </div>
                      ) : (
                        <span className="text-xs text-gray-400">—</span>
                      )}
                    </Td>
                    <Td><span className="text-xs">{formatDate(inst.startedAt)}</span></Td>
                    <Td><span className="text-xs">{inst.endedAt ? formatDate(inst.endedAt) : '—'}</span></Td>
                    <Td><Badge value={inst.status} /></Td>
                  </Tr>
                );
              })}
            </tbody>
          </Table>
        )}
      </Card>

      {selectedInstanceId && (
        <InstanceVariablesModal instanceId={selectedInstanceId} onClose={() => setSelectedInstanceId(null)} />
      )}
    </div>
  );
}

function InstanceVariablesModal({ instanceId, onClose }: { instanceId: string; onClose: () => void }) {
  const nav = useNavigate();
  const { data: variables, isLoading } = useQuery({
    queryKey: ['variables', instanceId],
    queryFn: () => getInstanceVariables(instanceId),
  });

  return (
    <Modal open onClose={onClose} title={`Variables: ${instanceId.slice(0, 8)}…`}>
      <div className="space-y-4">
        {isLoading ? (
          <div className="flex justify-center py-6"><Spinner className="w-5 h-5 text-blue-500" /></div>
        ) : !variables || Object.keys(variables).length === 0 ? (
          <p className="text-sm text-gray-400">No variables.</p>
        ) : (
          <Table>
            <Thead>
              <tr><Th>Name</Th><Th>Type</Th><Th>Value</Th></tr>
            </Thead>
            <tbody>
              {Object.entries(variables).map(([name, v]) => (
                <Tr key={name}>
                  <Td><span className="font-mono text-xs font-medium">{name}</span></Td>
                  <Td><span className="text-xs text-gray-500">{v.type}</span></Td>
                  <Td><span className="font-mono text-xs break-all">{String(v.value)}</span></Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        )}
        <div className="flex justify-end gap-2 pt-2 border-t border-gray-100">
          <Button variant="secondary" onClick={onClose}>Close</Button>
          <Button onClick={() => nav(`/instances/${instanceId}`)}>Open Instance</Button>
        </div>
      </div>
    </Modal>
  );
}
