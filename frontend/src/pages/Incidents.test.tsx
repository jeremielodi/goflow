import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import Incidents from './Incidents';
import * as incidentsApi from '../api/incidents';
import type { Incident } from '../types';

// Regression test: the Incident type/page used to read `incidentMessage`,
// `incidentTimestamp` and `configuration`, none of which exist on the real
// internal/models.Incident JSON (`errorMessage`, `createdAt`, `jobId`). The
// `configuration` bug was the worst of the three: it made "Retry" send the
// *incident's own* UUID as the job id, so POST /engine-rest/job/:id/retries
// silently updated zero rows.
vi.mock('../api/incidents');

const incident: Incident = {
  id: 'incident-1',
  processInstanceId: 'instance-1',
  jobId: 'job-42',
  incidentType: 'jobFailed',
  activityId: 'ServiceTask_1',
  errorMessage: 'connection refused',
  state: 'open',
  createdAt: '2026-07-02T10:00:00Z',
};

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <Incidents />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('Incidents page', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(incidentsApi.listIncidents).mockResolvedValue([incident]);
    vi.mocked(incidentsApi.retryJob).mockResolvedValue(undefined);
    vi.mocked(incidentsApi.deleteIncident).mockResolvedValue(undefined);
  });

  it('renders the error message and created time from the real API field names', async () => {
    renderPage();
    expect(await screen.findByText('connection refused')).toBeInTheDocument();
  });

  it('retries using the job id, not the incident id', async () => {
    renderPage();
    const retryButton = await screen.findByRole('button', { name: /retry/i });
    await userEvent.click(retryButton);
    expect(incidentsApi.retryJob).toHaveBeenCalledWith('job-42');
  });
});
