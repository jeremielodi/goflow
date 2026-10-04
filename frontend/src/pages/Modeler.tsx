import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation } from '@tanstack/react-query';
// @ts-ignore — bpmn-js doesn't ship ts declarations in all versions
import BpmnJS from 'bpmn-js/lib/Modeler';
import {
  BpmnPropertiesPanelModule,
  BpmnPropertiesProviderModule,
  ZeebePropertiesProviderModule,
  // @ts-ignore — bpmn-js-properties-panel doesn't ship ts declarations
} from 'bpmn-js-properties-panel';
// @ts-ignore — camunda-bpmn-js-behaviors doesn't ship ts declarations
import CamundaCloudBehaviorsModule from 'camunda-bpmn-js-behaviors/lib/camunda-cloud';
// @ts-ignore — no ts declarations for this JSON moddle descriptor
import ZeebeModdleExtension from 'zeebe-bpmn-moddle/resources/zeebe.json';
import { FilePlus, FolderOpen, Download, UploadCloud, ArrowLeft } from 'lucide-react';
import { Button, Spinner } from '../components/ui';
import { deployBpmn } from '../api/processes';

const BLANK_DIAGRAM = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
  xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"
  id="Definitions_1" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="Process_1" name="New Process" isExecutable="true">
    <bpmn:startEvent id="StartEvent_1" name="Started" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Process_1">
      <bpmndi:BPMNShape id="StartEvent_1_di" bpmnElement="StartEvent_1">
        <dc:Bounds x="152" y="102" width="36" height="36" />
      </bpmndi:BPMNShape>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

// Extracts the bpmn:process id, used as the deployed filename — GoFlow's
// process key comes from this XML attribute, not the filename itself, but
// naming the file after it keeps deployments recognizable in the
// definitions list's "resource name" column.
function extractProcessId(xml: string): string {
  const match = xml.match(/<bpmn:process[^>]*\sid="([^"]+)"/);
  return match?.[1] ?? 'process';
}

export default function ModelerPage() {
  const navigate = useNavigate();
  const containerRef = useRef<HTMLDivElement>(null);
  const propertiesPanelRef = useRef<HTMLDivElement>(null);
  const modelerRef = useRef<InstanceType<typeof BpmnJS> | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [ready, setReady] = useState(false);
  const [importError, setImportError] = useState<string | null>(null);
  const [deploySuccess, setDeploySuccess] = useState(false);

  useEffect(() => {
    if (!containerRef.current || !propertiesPanelRef.current) return;

    const modeler = new BpmnJS({
      container: containerRef.current,
      propertiesPanel: { parent: propertiesPanelRef.current },
      additionalModules: [
        BpmnPropertiesPanelModule,
        BpmnPropertiesProviderModule,
        ZeebePropertiesProviderModule,
        CamundaCloudBehaviorsModule,
      ],
      moddleExtensions: { zeebe: ZeebeModdleExtension },
    });
    modelerRef.current = modeler;

    (modeler.importXML(BLANK_DIAGRAM) as Promise<unknown>)
      .then(() => {
        (modeler.get('canvas') as { zoom(a: string, b: string): void }).zoom('fit-viewport', 'auto');
        setReady(true);
      })
      .catch(() => setImportError('Failed to initialize a blank diagram'));

    return () => { modeler.destroy(); };
  }, []);

  const deployMut = useMutation({
    mutationFn: async () => {
      const modeler = modelerRef.current;
      if (!modeler) throw new Error('modeler not ready');
      const { xml } = await (modeler.saveXML({ format: true }) as Promise<{ xml: string }>);
      const processId = extractProcessId(xml);
      const file = new File([xml], `${processId}.bpmn`, { type: 'application/xml' });
      await deployBpmn(file);
    },
    onSuccess: () => {
      setDeploySuccess(true);
      setTimeout(() => setDeploySuccess(false), 4000);
    },
  });

  const handleNew = () => {
    if (!modelerRef.current) return;
    setImportError(null);
    (modelerRef.current.importXML(BLANK_DIAGRAM) as Promise<unknown>)
      .then(() => (modelerRef.current!.get('canvas') as { zoom(a: string, b: string): void }).zoom('fit-viewport', 'auto'))
      .catch(() => setImportError('Failed to create a new diagram'));
  };

  const handleImportFile = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = '';
    if (!file || !modelerRef.current) return;
    setImportError(null);
    file.text().then((xml) => {
      (modelerRef.current!.importXML(xml) as Promise<unknown>)
        .then(() => (modelerRef.current!.get('canvas') as { zoom(a: string, b: string): void }).zoom('fit-viewport', 'auto'))
        .catch(() => setImportError('That file is not a valid BPMN diagram'));
    });
  };

  const handleExport = async () => {
    if (!modelerRef.current) return;
    const { xml } = await (modelerRef.current.saveXML({ format: true }) as Promise<{ xml: string }>);
    const processId = extractProcessId(xml);
    const blob = new Blob([xml], { type: 'application/xml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${processId}.bpmn`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="flex flex-col h-screen">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200 bg-white flex-shrink-0">
        <div className="flex items-center gap-3">
          <button
            onClick={() => navigate('/processes')}
            className="text-gray-400 hover:text-gray-700"
            title="Back to definitions"
          >
            <ArrowLeft className="w-5 h-5" />
          </button>
          <h1 className="text-base font-semibold text-gray-900">BPMN Modeler</h1>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="secondary" size="sm" onClick={handleNew}>
            <FilePlus className="w-4 h-4" /> New
          </Button>
          <Button variant="secondary" size="sm" onClick={() => fileInputRef.current?.click()}>
            <FolderOpen className="w-4 h-4" /> Import
          </Button>
          <input ref={fileInputRef} type="file" accept=".bpmn,.xml" className="hidden" onChange={handleImportFile} />
          <Button variant="secondary" size="sm" onClick={handleExport}>
            <Download className="w-4 h-4" /> Export
          </Button>
          <Button size="sm" onClick={() => deployMut.mutate()} disabled={deployMut.isPending}>
            {deployMut.isPending ? <Spinner className="w-4 h-4" /> : <UploadCloud className="w-4 h-4" />}
            Deploy
          </Button>
        </div>
      </div>

      {(importError || deployMut.isError || deploySuccess) && (
        <div className={`px-4 py-2 text-sm flex-shrink-0 ${
          importError || deployMut.isError ? 'bg-red-50 text-red-700' : 'bg-green-50 text-green-700'
        }`}>
          {importError ?? (deployMut.isError ? 'Deploy failed — check the diagram is complete and valid.' : 'Deployed successfully.')}
        </div>
      )}

      <div className="flex-1 flex min-h-0 relative">
        {!ready && (
          <div className="absolute inset-0 flex items-center justify-center bg-white z-10">
            <Spinner className="w-8 h-8 text-blue-500" />
          </div>
        )}
        <div ref={containerRef} className="flex-1 min-w-0" />
        <div ref={propertiesPanelRef} className="w-80 flex-shrink-0 border-l border-gray-200 overflow-y-auto bg-white" />
      </div>
    </div>
  );
}
