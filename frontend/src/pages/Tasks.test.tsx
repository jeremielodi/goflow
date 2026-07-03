import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import Tasks from './Tasks';
import * as tasksApi from '../api/tasks';
import * as formsApi from '../api/forms';
import { useAuth } from '../hooks/useAuth';
import type { UserTask } from '../types';

vi.mock('../api/tasks');
vi.mock('../api/forms');
vi.mock('../hooks/useAuth');

const baseTask: UserTask = {
  id: 'task-1',
  taskName: 'Review order',
  status: 'created',
  createdAt: '2026-07-02T10:00:00Z',
  processInstanceId: 'inst-1',
  taskDefinitionKey: 'ReviewTask',
};

function renderPage() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <Tasks />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('Tasks page - complete task modal', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(useAuth).mockReturnValue({
      user: { id: 'u1', email: 'admin@goflow.com' },
      roles: [],
      actions: [],
      isLoading: false,
      login: vi.fn(),
      logout: vi.fn(),
      hasAction: vi.fn().mockReturnValue(false),
    });
    vi.mocked(tasksApi.completeTask).mockResolvedValue(undefined);
    vi.mocked(tasksApi.claimTask).mockResolvedValue(undefined);
    vi.mocked(tasksApi.unclaimTask).mockResolvedValue(undefined);
  });

  it('falls back to a raw JSON textarea when the task has no linked form', async () => {
    vi.mocked(tasksApi.listTasks).mockResolvedValue([baseTask]);

    renderPage();
    await userEvent.click(await screen.findByRole('button', { name: /complete/i }));

    expect(screen.getByText(/output variables \(json\)/i)).toBeInTheDocument();
    expect(formsApi.getForm).not.toHaveBeenCalled();
  });

  it('renders real form fields for a task with a linked form and submits their values', async () => {
    const taskWithForm: UserTask = { ...baseTask, formKey: 'review-form' };
    vi.mocked(tasksApi.listTasks).mockResolvedValue([taskWithForm]);
    vi.mocked(formsApi.getForm).mockResolvedValue({
      formId: 'review-form',
      version: 1,
      schema: {
        components: [{ key: 'approved', type: 'checkbox', label: 'Approved?' }],
      },
    });

    renderPage();
    await userEvent.click(await screen.findByRole('button', { name: /complete/i }));

    expect(await screen.findByText('Approved?')).toBeInTheDocument();
    const checkbox = screen.getByRole('checkbox');
    expect(screen.queryByText(/output variables \(json\)/i)).not.toBeInTheDocument();

    await userEvent.click(checkbox);
    await userEvent.click(screen.getByRole('button', { name: /complete task/i }));

    expect(tasksApi.completeTask).toHaveBeenCalledWith('task-1', { approved: true });
  });
});

// Regression: previously there was no status indicator at all, so a
// completed task (status: "completed", completedAt already set on the wire)
// rendered identically to an open one, with an active "Complete" button.
describe('Tasks page - status badge', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(useAuth).mockReturnValue({
      user: { id: 'u1', email: 'admin@goflow.com' },
      roles: [],
      actions: [],
      isLoading: false,
      login: vi.fn(),
      logout: vi.fn(),
      hasAction: vi.fn().mockReturnValue(false),
    });
  });

  it('shows the exact task_status DB value as the badge and keeps action buttons for an open task', async () => {
    vi.mocked(tasksApi.listTasks).mockResolvedValue([{ ...baseTask, status: 'claimed', assignee: 'admin@goflow.com' }]);
    renderPage();

    expect(await screen.findByText('claimed')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /complete/i })).toBeInTheDocument();
  });

  it('hides Claim/Complete actions for a completed task instead of showing it as open', async () => {
    vi.mocked(tasksApi.listTasks).mockResolvedValue([{ ...baseTask, status: 'completed', assignee: 'admin@goflow.com' }]);
    renderPage();

    expect(await screen.findByText('completed')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /complete/i })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /claim/i })).not.toBeInTheDocument();
  });
});
