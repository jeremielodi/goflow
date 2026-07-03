import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts';
import { TrendingUp, AlertTriangle, Activity } from 'lucide-react';
import { getProcessStats } from '../api/analytics';
import { listDefinitions } from '../api/processes';
import { Card, CardHeader, Table, Thead, Th, Tr, Td } from '../components/ui';
import { formatDuration } from '../utils/format';

// Categorical palette (fixed order — never cycled/reassigned per filter,
// see the dataviz skill's color-formula.md).
const SERIES = {
  blue: '#2a78d6',
  aqua: '#1baf7a',
  yellow: '#eda100',
};
const INK_MUTED = '#898781';
const GRIDLINE = '#e1e0d9';

const DAY_OPTIONS = [
  { label: 'Last 7 days', value: 7 },
  { label: 'Last 30 days', value: 30 },
  { label: 'Last 90 days', value: 90 },
];

export default function Analytics() {
  const [processKey, setProcessKey] = useState('');
  const [days, setDays] = useState(30);

  const { data: defs = [] } = useQuery({ queryKey: ['definitions'], queryFn: () => listDefinitions() });

  const { from, to } = useMemo(() => {
    const to = new Date();
    const from = new Date(to);
    from.setDate(from.getDate() - days);
    return { from: from.toISOString(), to: to.toISOString() };
  }, [days]);

  const { data, isLoading } = useQuery({
    queryKey: ['analytics', 'process-stats', processKey, from, to],
    queryFn: () => getProcessStats({ processKey: processKey || undefined, from, to }),
  });

  const byProcessKey = data?.byProcessKey ?? [];
  const throughput = data?.throughput ?? [];

  const uniqueKeys = useMemo(
    () => Array.from(new Set(defs.map(d => d.key))).sort(),
    [defs]
  );

  const totals = useMemo(() => {
    const totalStarted = byProcessKey.reduce((sum, s) => sum + s.runningCount + s.completedCount + s.terminatedCount, 0);
    const totalIncidents = byProcessKey.reduce((sum, s) => sum + s.incidentCount, 0);
    const durations = byProcessKey.filter(s => s.avgDurationMillis != null);
    const avgDuration = durations.length > 0
      ? durations.reduce((sum, s) => sum + (s.avgDurationMillis ?? 0), 0) / durations.length
      : null;
    return {
      totalStarted,
      incidentRate: totalStarted > 0 ? totalIncidents / totalStarted : 0,
      avgDuration,
    };
  }, [byProcessKey]);

  const incidentRateData = useMemo(
    () => [...byProcessKey].sort((a, b) => b.incidentRate - a.incidentRate),
    [byProcessKey]
  );

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Analytics</h1>
          <p className="text-sm text-gray-500 mt-0.5">Duration, throughput, and incident rate by process</p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={processKey}
            onChange={e => setProcessKey(e.target.value)}
            className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 text-gray-700 bg-white"
          >
            <option value="">All processes</option>
            {uniqueKeys.map(key => (
              <option key={key} value={key}>{key}</option>
            ))}
          </select>
          <select
            value={days}
            onChange={e => setDays(Number(e.target.value))}
            className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 text-gray-700 bg-white"
          >
            {DAY_OPTIONS.map(opt => (
              <option key={opt.value} value={opt.value}>{opt.label}</option>
            ))}
          </select>
        </div>
      </div>

      {/* KPI row */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <Card className="p-4">
          <div className="w-9 h-9 rounded-lg bg-blue-50 flex items-center justify-center mb-3">
            <Activity className="w-5 h-5 text-blue-500" />
          </div>
          <p className="text-2xl font-bold text-gray-900">{totals.totalStarted}</p>
          <p className="text-xs text-gray-500 mt-0.5">Started in range</p>
        </Card>
        <Card className="p-4">
          <div className="w-9 h-9 rounded-lg bg-red-50 flex items-center justify-center mb-3">
            <AlertTriangle className="w-5 h-5 text-red-500" />
          </div>
          <p className="text-2xl font-bold text-gray-900">{(totals.incidentRate * 100).toFixed(1)}%</p>
          <p className="text-xs text-gray-500 mt-0.5">Overall incident rate</p>
        </Card>
        <Card className="p-4">
          <div className="w-9 h-9 rounded-lg bg-green-50 flex items-center justify-center mb-3">
            <TrendingUp className="w-5 h-5 text-green-500" />
          </div>
          <p className="text-2xl font-bold text-gray-900">
            {totals.avgDuration != null ? formatDuration(totals.avgDuration) : '—'}
          </p>
          <p className="text-xs text-gray-500 mt-0.5">Avg duration (across processes)</p>
        </Card>
      </div>

      {/* Throughput over time */}
      <Card>
        <CardHeader title="Throughput over time" subtitle="Instances started vs. completed per day" />
        <div className="px-6 pb-6 pt-2" style={{ height: 280 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={throughput}>
              <CartesianGrid stroke={GRIDLINE} vertical={false} />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: INK_MUTED }} tickLine={false} axisLine={{ stroke: GRIDLINE }} />
              <YAxis tick={{ fontSize: 11, fill: INK_MUTED }} tickLine={false} axisLine={false} allowDecimals={false} />
              <Tooltip />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Line type="monotone" dataKey="started" name="Started" stroke={SERIES.blue} strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="completed" name="Completed" stroke={SERIES.aqua} strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Duration distribution */}
        <Card>
          <CardHeader title="Duration distribution" subtitle="Average / p50 / p95 per process key" />
          <div className="px-6 pb-6 pt-2" style={{ height: 300 }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={byProcessKey}>
                <CartesianGrid stroke={GRIDLINE} vertical={false} />
                <XAxis dataKey="processKey" tick={{ fontSize: 11, fill: INK_MUTED }} tickLine={false} axisLine={{ stroke: GRIDLINE }} />
                <YAxis tick={{ fontSize: 11, fill: INK_MUTED }} tickLine={false} axisLine={false} tickFormatter={v => formatDuration(v)} />
                <Tooltip formatter={(v: unknown) => formatDuration(Number(v ?? 0))} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                <Bar dataKey="avgDurationMillis" name="Avg" fill={SERIES.blue} radius={[4, 4, 0, 0]} />
                <Bar dataKey="p50DurationMillis" name="p50" fill={SERIES.aqua} radius={[4, 4, 0, 0]} />
                <Bar dataKey="p95DurationMillis" name="p95" fill={SERIES.yellow} radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>

        {/* Incident rate by process key */}
        <Card>
          <CardHeader title="Incident rate by process" subtitle="Sorted, highest risk first" />
          <div className="px-6 pb-6 pt-2" style={{ height: 300 }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={incidentRateData} layout="vertical" margin={{ left: 16 }}>
                <CartesianGrid stroke={GRIDLINE} horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 11, fill: INK_MUTED }} tickLine={false} axisLine={{ stroke: GRIDLINE }} tickFormatter={v => `${(v * 100).toFixed(0)}%`} />
                <YAxis type="category" dataKey="processKey" tick={{ fontSize: 11, fill: INK_MUTED }} tickLine={false} axisLine={false} width={110} />
                <Tooltip formatter={(v: unknown) => `${(Number(v ?? 0) * 100).toFixed(1)}%`} />
                <Bar dataKey="incidentRate" name="Incident rate" fill={SERIES.blue} radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>
      </div>

      {/* Raw numbers */}
      <Card>
        <CardHeader title="By process key" subtitle="Exact values" />
        {isLoading ? (
          <p className="text-sm text-gray-400 px-6 py-8 text-center">Loading…</p>
        ) : byProcessKey.length === 0 ? (
          <p className="text-sm text-gray-400 px-6 py-8 text-center">No data in this range</p>
        ) : (
          <Table>
            <Thead>
              <tr>
                <Th>Process</Th><Th>Running</Th><Th>Completed</Th><Th>Terminated</Th>
                <Th>Avg</Th><Th>p50</Th><Th>p95</Th><Th>Incidents</Th><Th>Rate</Th>
              </tr>
            </Thead>
            <tbody>
              {byProcessKey.map(s => (
                <Tr key={s.processKey}>
                  <Td><span className="font-medium text-xs">{s.processKey}</span></Td>
                  <Td><span className="text-xs">{s.runningCount}</span></Td>
                  <Td><span className="text-xs">{s.completedCount}</span></Td>
                  <Td><span className="text-xs">{s.terminatedCount}</span></Td>
                  <Td><span className="text-xs">{s.avgDurationMillis != null ? formatDuration(s.avgDurationMillis) : '—'}</span></Td>
                  <Td><span className="text-xs">{s.p50DurationMillis != null ? formatDuration(s.p50DurationMillis) : '—'}</span></Td>
                  <Td><span className="text-xs">{s.p95DurationMillis != null ? formatDuration(s.p95DurationMillis) : '—'}</span></Td>
                  <Td><span className="text-xs">{s.incidentCount}</span></Td>
                  <Td><span className="text-xs">{(s.incidentRate * 100).toFixed(1)}%</span></Td>
                </Tr>
              ))}
            </tbody>
          </Table>
        )}
      </Card>
    </div>
  );
}
