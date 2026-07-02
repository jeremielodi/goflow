import { useRef, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { Upload, Play, GitBranch, RefreshCw, ChevronRight } from 'lucide-react';
import { listDefinitions, deployBpmn, startProcess } from '../api/processes';
import { Card, CardHeader, Table, Thead, Th, Tr, Td, Badge, Button, Modal, Input, Spinner, EmptyState } from '../components/ui';
import { formatDate } from '../utils/format';

export default function ProcessDefinitions() {
  const qc = useQueryClient();
  const nav = useNavigate();
  const fileRef = useRef<HTMLInputElement>(null);

  const [deployOpen, setDeployOpen] = useState(false);
  const [startOpen, setStartOpen]   = useState<string | null>(null);
  const [startVars, setStartVars]   = useState('{}');
  const [startError, setStartError] = useState('');

  const { data: defs = [], isLoading, refetch } = useQuery({
    queryKey: ['definitions'],
    queryFn: () => listDefinitions(),
  });

  const deployMut = useMutation({
    mutationFn: (file: File) => deployBpmn(file),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['definitions'] }); setDeployOpen(false); },
  });

  const startMut = useMutation({
    mutationFn: ({ key, vars }: { key: string; vars: Record<string, unknown> }) =>
      startProcess(key, vars),
    onSuccess: (data) => {
      qc.invalidateQueries({ queryKey: ['instances'] });
      setStartOpen(null);
      nav(`/instances/${data.processInstanceId}`);
    },
  });

  const handleDeploy = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) deployMut.mutate(file);
  };

  const handleStart = () => {
    if (!startOpen) return;
    setStartError('');
    try {
      const vars = JSON.parse(startVars);
      startMut.mutate({ key: startOpen, vars });
    } catch {
      setStartError('Invalid JSON');
    }
  };

  // Deduplicate: show latest version of each key
  const latest = Object.values(
    defs.reduce<Record<string, (typeof defs)[0]>>((acc, d) => {
      if (!acc[d.key] || d.version > acc[d.key].version) acc[d.key] = d;
      return acc;
    }, {})
  );

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Process Definitions</h1>
          <p className="text-sm text-gray-500 mt-0.5">{defs.length} deployed definitions</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="sm" onClick={() => refetch()}>
            <RefreshCw className="w-4 h-4" />
          </Button>
          <Button variant="secondary" size="sm" onClick={() => setDeployOpen(true)}>
            <Upload className="w-4 h-4" /> Deploy BPMN
          </Button>
        </div>
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12">
            <Spinner className="w-6 h-6 text-blue-500" />
          </div>
        ) : latest.length === 0 ? (
          <EmptyState
            icon={<GitBranch className="w-12 h-12" />}
            title="No process definitions"
            description="Deploy a BPMN file to get started"
          />
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>Name / Key</Th>
                <Th>Version</Th>
                <Th>Deployed</Th>
                <Th></Th>
              </tr>
            </Thead>
            <tbody>
              {latest.map(def => (
                <Tr key={def.id}>
                  <Td>
                    <p className="font-medium text-gray-900">{def.name || def.key}</p>
                    <p className="text-xs text-gray-400 font-mono">{def.key}</p>
                  </Td>
                  <Td>
                    <Badge value="default" label={`v${def.version}`} />
                    {def.version > 1 && (
                      <button
                        onClick={() => nav(`/processes/${def.key}/versions`)}
                        className="ml-2 text-xs text-blue-600 hover:underline"
                      >
                        {def.version} versions
                      </button>
                    )}
                  </Td>
                  <Td><span className="text-xs">{formatDate(def.createdAt)}</span></Td>
                  <Td>
                    <div className="flex items-center gap-2 justify-end">
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() => nav(`/instances?key=${def.key}`)}
                      >
                        <Play className="w-3.5 h-3.5" /> Instances
                      </Button>
                      <Button
                        size="sm"
                        onClick={() => { setStartOpen(def.key); setStartVars('{}'); setStartError(''); }}
                      >
                        <Play className="w-3.5 h-3.5" /> Start
                      </Button>
                      <button
                        onClick={() => nav(`/processes/${def.id}`)}
                        className="text-gray-400 hover:text-gray-600 p-1"
                      >
                        <ChevronRight className="w-4 h-4" />
                      </button>
                    </div>
                  </Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        )}
      </Card>

      {/* Deploy modal */}
      <Modal open={deployOpen} onClose={() => setDeployOpen(false)} title="Deploy BPMN Process">
        <div className="space-y-4">
          <div
            className="border-2 border-dashed border-gray-300 rounded-xl p-8 text-center cursor-pointer hover:border-blue-400 transition-colors"
            onClick={() => fileRef.current?.click()}
          >
            <Upload className="w-8 h-8 text-gray-400 mx-auto mb-2" />
            <p className="text-sm font-medium text-gray-700">Click to select a BPMN file</p>
            <p className="text-xs text-gray-400 mt-1">or drag and drop</p>
            <input ref={fileRef} type="file" accept=".bpmn,.xml" className="hidden" onChange={handleDeploy} />
          </div>
          {deployMut.isPending && (
            <div className="flex items-center justify-center gap-2 text-sm text-gray-500">
              <Spinner className="w-4 h-4 text-blue-500" /> Deploying…
            </div>
          )}
          {deployMut.isError && (
            <p className="text-sm text-red-600">Deploy failed. Check your BPMN file.</p>
          )}
        </div>
      </Modal>

      {/* Start instance modal */}
      <Modal open={!!startOpen} onClose={() => setStartOpen(null)} title={`Start: ${startOpen}`}>
        <div className="space-y-4">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1.5">
              Initial Variables (JSON)
            </label>
            <textarea
              value={startVars}
              onChange={e => setStartVars(e.target.value)}
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono h-28 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            {startError && <p className="text-xs text-red-500 mt-1">{startError}</p>}
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="secondary" onClick={() => setStartOpen(null)}>Cancel</Button>
            <Button onClick={handleStart} disabled={startMut.isPending}>
              {startMut.isPending && <Spinner className="w-4 h-4 text-white" />}
              Start Process
            </Button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
