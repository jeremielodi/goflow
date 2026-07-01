import { useEffect, useRef } from 'react';
// @ts-ignore — bpmn-js doesn't ship ts declarations in all versions
import BpmnJS from 'bpmn-js/lib/NavigatedViewer';
import type { HistoricActivityInstance } from '../../types';

interface BpmnViewerProps {
  xml: string;
  activities?: HistoricActivityInstance[];
  className?: string;
}

// Color tokens by state
const TOKEN_ACTIVE    = '#3b82f6';   // blue-500
const TOKEN_COMPLETED = '#22c55e';   // green-500
const TOKEN_CANCELED  = '#f59e0b';   // amber-500

export function BpmnViewer({ xml, activities = [], className = '' }: BpmnViewerProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewerRef    = useRef<InstanceType<typeof BpmnJS> | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const viewer = new BpmnJS({ container: containerRef.current });
    viewerRef.current = viewer;
    return () => { viewer.destroy(); };
  }, []);

  // Re-import XML whenever it changes
  useEffect(() => {
    if (!viewerRef.current || !xml) return;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (viewerRef.current.importXML(xml) as Promise<unknown>).then(() => {
      (viewerRef.current! as any).get('canvas').zoom('fit-viewport', 'auto');
    }).catch(() => {/* invalid xml */ });
  }, [xml]);

  // Overlay token badges whenever activities change
  useEffect(() => {
    if (!viewerRef.current || !activities.length) return;

    const overlays = viewerRef.current.get('overlays') as {
      add(elementId: string, opts: object): void;
      clear(elementId: string, type: string): void;
    };
    const elementRegistry = viewerRef.current.get('elementRegistry') as {
      get(id: string): unknown;
    };

    // Clear old token overlays
    try {
      activities.forEach((a) => {
        try { overlays.clear(a.activityId, 'token'); } catch { /* element may not exist */ }
      });
    } catch { /* ignore */ }

    // Group activities by element ID (there may be multiple executions at same node)
    const byElement = new Map<string, HistoricActivityInstance[]>();
    for (const act of activities) {
      if (!byElement.has(act.activityId)) byElement.set(act.activityId, []);
      byElement.get(act.activityId)!.push(act);
    }

    for (const [elementId, acts] of byElement.entries()) {
      if (!elementRegistry.get(elementId)) continue;

      // Determine dominant state for the badge color
      const hasActive    = acts.some(a => !a.endTime && !a.canceled);
      const hasCanceled  = acts.some(a => a.canceled);
      const color = hasActive ? TOKEN_ACTIVE : hasCanceled ? TOKEN_CANCELED : TOKEN_COMPLETED;
      const count = acts.length;

      const html = `
        <div style="
          background:${color};color:#fff;
          border-radius:50%;width:20px;height:20px;
          display:flex;align-items:center;justify-content:center;
          font-size:11px;font-weight:700;
          box-shadow:0 1px 3px rgba(0,0,0,.3);
          pointer-events:none;
        ">${count > 9 ? '9+' : count}</div>`;

      overlays.add(elementId, {
        type: 'token',
        position: { top: -10, right: -10 },
        html,
      });

      // Also highlight the shape border
      try {
        const canvas = viewerRef.current!.get('canvas') as {
          addMarker(elementId: string, marker: string): void;
          removeMarker(elementId: string, marker: string): void;
        };
        canvas.removeMarker(elementId, 'highlight-active');
        canvas.removeMarker(elementId, 'highlight-completed');
        canvas.addMarker(elementId, hasActive ? 'highlight-active' : 'highlight-completed');
      } catch { /* ignore */ }
    }
  }, [activities]);

  return (
    <div className={`relative ${className}`}>
      <style>{`
        .highlight-active .djs-visual > :is(rect,circle,polygon,path) {
          stroke: ${TOKEN_ACTIVE} !important;
          stroke-width: 3px !important;
        }
        .highlight-completed .djs-visual > :is(rect,circle,polygon,path) {
          stroke: ${TOKEN_COMPLETED} !important;
          stroke-width: 2px !important;
          opacity: 0.8;
        }
      `}</style>
      <div ref={containerRef} className="w-full h-full" />
    </div>
  );
}
