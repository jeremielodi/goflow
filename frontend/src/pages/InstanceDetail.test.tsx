import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import InstanceDetail from './InstanceDetail';
import * as processesApi from '../api/processes';
import * as historyApi from '../api/history';
import * as auditApi from '../api/audit';
import * as tasksApi from '../api/tasks';
import type { ProcessInstance, HistoricActivityInstance, TokenHistoryStep } from '../types';

// Regression test: the live ProcessInstance JSON has no `durationInMillis`
// field (that only exists on the separate historic-instance shape) — it must
// be derived client-side from startedAt/endedAt, otherwise the "Duration"
// stat always rendered "—" even for completed instances.
vi.mock('../api/processes');
vi.mock('../api/history');
vi.mock('../api/audit');
vi.mock('../api/tasks');
vi.mock('../api/client', () => ({
  apiClient: { get: vi.fn().mockRejectedValue(new Error('not needed for this test')) },
}));

const runningInstance: ProcessInstance = {
  id: 'inst-1',
  processDefinitionId: 'def-1',
  status: 'running',
  startedAt: '2026-07-02T10:00:00.000Z',
  processKey: 'OrderProcess',
};

const completedInstance: ProcessInstance = {
  ...runningInstance,
  status: 'completed',
  endedAt: '2026-07-02T10:01:05.000Z', // +65s
};

const activeActivity: HistoricActivityInstance = {
  id: 'act-1',
  activityId: 'ReviewTask',
  activityType: 'userTask',
  processInstanceId: 'inst-1',
  processDefinitionId: 'def-1',
  startTime: '2026-07-02T10:00:00.000Z',
  canceled: false,
  completeScope: false,
};

const tokenHistorySteps: TokenHistoryStep[] = [
  { timestamp: '2026-07-02T10:00:00.000Z', action: 'PROCESS_STARTED', detail: 'Process instance started' },
  {
    timestamp: '2026-07-02T10:00:01.000Z',
    action: 'TASK_CLAIMED',
    elementId: 'ReviewTask',
    elementName: 'Review',
    detail: 'Claimed by alice',
  },
];

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={['/instances/inst-1']}>
        <Routes>
          <Route path="/instances/:id" element={<InstanceDetail />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('InstanceDetail page', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(historyApi.listHistoricActivities).mockResolvedValue([activeActivity]);
    vi.mocked(auditApi.getTokenHistory).mockResolvedValue(tokenHistorySteps);
    vi.mocked(tasksApi.listTasks).mockResolvedValue([]);
    vi.mocked(processesApi.getInstanceVariables).mockResolvedValue({});
    vi.mocked(processesApi.modifyInstance).mockResolvedValue({ newExecutionIds: ['exec-2'] });
  });

  it('shows a dash for duration on a running instance instead of a bogus API field', async () => {
    vi.mocked(processesApi.getInstance).mockResolvedValue(runningInstance);
    renderPage();

    await screen.findByText('OrderProcess');
    const durationValue = screen.getAllByText('Duration')[0].nextElementSibling;
    expect(durationValue).toHaveTextContent('—');
  });

  it('computes duration client-side from startedAt/endedAt for a completed instance', async () => {
    vi.mocked(processesApi.getInstance).mockResolvedValue(completedInstance);
    renderPage();

    await screen.findByText('OrderProcess');
    const durationValue = screen.getAllByText('Duration')[0].nextElementSibling;
    expect(durationValue).toHaveTextContent('1m 5s');
  });

  it('moves a token by calling modifyInstance with the picked source/target elements', async () => {
    vi.mocked(processesApi.getInstance).mockResolvedValue(runningInstance);
    renderPage();

    const openButtons = await screen.findAllByRole('button', { name: /move token/i });
    await userEvent.click(openButtons[0]);

    await userEvent.type(screen.getByPlaceholderText(/approveTask/i), 'ApproveTask');

    const allButtons = screen.getAllByRole('button', { name: /move token/i });
    await userEvent.click(allButtons[allButtons.length - 1]);

    expect(processesApi.modifyInstance).toHaveBeenCalledWith('inst-1', 'ReviewTask', 'ApproveTask');
  });

  // Regression test: "Activity History" used to be sourced from the live
  // executions table (via listHistoricActivities), which can only show a
  // token's current position, not its path. It must render the audit-log-
  // derived token history instead, including entries with no elementId
  // (e.g. PROCESS_STARTED).
  it('renders the audit-log-derived token history, not just current execution state', async () => {
    vi.mocked(processesApi.getInstance).mockResolvedValue(runningInstance);
    renderPage();

    await screen.findByText('Process instance started');
    expect(screen.getByText('Claimed by alice')).toBeInTheDocument();
    expect(screen.getByText('Review')).toBeInTheDocument();
    expect(screen.getByText('2 recorded events')).toBeInTheDocument();
  });
});
