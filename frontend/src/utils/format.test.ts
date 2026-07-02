import { describe, it, expect } from 'vitest';
import { formatDate, formatDuration, truncateId } from './format';

describe('formatDate', () => {
  it('returns an em dash for missing input', () => {
    expect(formatDate(undefined)).toBe('—');
    expect(formatDate(null)).toBe('—');
    expect(formatDate('')).toBe('—');
  });

  it('formats an ISO string into a short date', () => {
    const result = formatDate('2026-07-02T10:30:00Z');
    expect(result).toMatch(/Jul/);
    expect(result).not.toBe('—');
  });
});

describe('formatDuration', () => {
  it('formats sub-second durations in ms', () => {
    expect(formatDuration(500)).toBe('500ms');
  });

  it('formats sub-minute durations in seconds', () => {
    expect(formatDuration(1500)).toBe('1.5s');
  });

  it('formats sub-hour durations in minutes and seconds', () => {
    expect(formatDuration(65_000)).toBe('1m 5s');
  });

  it('formats long durations in hours and minutes', () => {
    expect(formatDuration(3_660_000)).toBe('1h 1m');
  });
});

describe('truncateId', () => {
  it('returns an em dash for missing input', () => {
    expect(truncateId(undefined)).toBe('—');
  });

  it('truncates to the default length with an ellipsis', () => {
    expect(truncateId('0123456789abcdef')).toBe('01234567…');
  });

  it('respects a custom length', () => {
    expect(truncateId('0123456789abcdef', 4)).toBe('0123…');
  });
});
