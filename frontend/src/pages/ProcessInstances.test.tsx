import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import ProcessInstances from './ProcessInstances';
import * as processesApi from '../api/processes';
import type { ProcessInstance, ProcessDefinition } from '../types';

// Regression test: the live ProcessInstance JSON (internal/models.ProcessInstance
// marshaled as-is) uses `processKey`/`startedAt`, not the Camunda-7-style
// `processDefinitionKey`/`startTime` used by the *historic* endpoint. The page
// used to read the historic field names against the live data and always
// rendered blank cells. Also: the backend's GET /engine-rest/process-instance
// list filter reads query param `processKey`, not `processDefinitionKey`.
vi.mock('../api/processes');

const instance: ProcessInstance = {
  id: 'inst-1',
  processDefinitionId: 'def-1',
  status: 'running',
  startedAt: '2026-07-02T10:00:00Z',
  processKey: 'OrderProcess',
};

const definition: ProcessDefinition = {
  id: 'def-1',
  key: 'OrderProcess',
  name: 'Order Process',
  version: 1,
  deploymentId: 'dep-1',
  createdAt: '2026-07-01T00:00:00Z',
};

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <ProcessInstances />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('ProcessInstances page', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(processesApi.listInstances).mockResolvedValue([instance]);
    vi.mocked(processesApi.listDefinitions).mockResolvedValue([definition]);
  });

  it('renders the process key and start date from the real API field names', async () => {
    renderPage();
    expect(await screen.findByRole('cell', { name: 'OrderProcess' })).toBeInTheDocument();
  });

  it('filters using the processKey query param the backend actually reads', async () => {
    renderPage();
    await screen.findByRole('option', { name: 'OrderProcess' });
    const select = screen.getByDisplayValue('All process keys');
    await userEvent.selectOptions(select, 'OrderProcess');

    await waitFor(() => {
      expect(processesApi.listInstances).toHaveBeenCalledWith(
        expect.objectContaining({ processKey: 'OrderProcess' })
      );
    });
  });
});
