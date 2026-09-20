/**
 * Shell and formatting tests.
 *
 * The formatting assertions are the important ones: they pin down the rule that
 * an absent measurement renders as a dash and never as a zero. That rule is the
 * difference between a dashboard that reports and one that guesses.
 */

import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { SystemHealthPage } from '@/pages/SystemHealth';
import { ConnectionBadge, HealthBadge, ModeBadge } from '@/components/StatusIndicator';
import {
  NOT_AVAILABLE,
  formatAge,
  formatGap,
  formatLapTime,
  formatLatency,
  formatSector,
  teamColour,
} from '@/lib/format';
import type { HealthResponse } from '@/lib/types';

describe('formatting: absent values never become zero', () => {
  it.each([
    ['lap time', formatLapTime],
    ['sector', formatSector],
    ['latency', formatLatency],
    ['age', formatAge],
  ])('%s renders a dash for null and undefined', (_name, fn) => {
    expect(fn(null)).toBe(NOT_AVAILABLE);
    expect(fn(undefined)).toBe(NOT_AVAILABLE);
    expect(fn(Number.NaN)).toBe(NOT_AVAILABLE);
  });

  it('distinguishes a measured zero from an absent value', () => {
    // 0 ms is a real measurement and must not be hidden behind a dash.
    expect(formatLatency(0)).not.toBe(NOT_AVAILABLE);
    expect(formatLatency(null)).toBe(NOT_AVAILABLE);
  });
});

describe('formatting: motorsport conventions', () => {
  it('formats lap times as m:ss.SSS', () => {
    expect(formatLapTime(91.204)).toBe('1:31.204');
    expect(formatLapTime(62.5)).toBe('1:02.500');
    expect(formatLapTime(45.123)).toBe('45.123');
  });

  it('formats gaps with an explicit sign, and the leader as LEADER', () => {
    expect(formatGap(0)).toBe('LEADER');
    expect(formatGap(2.104)).toBe('+2.104');
    expect(formatGap(null, 1)).toBe('+1 LAP');
    expect(formatGap(null, 2)).toBe('+2 LAPS');
    // A lapped car has no meaningful seconds gap; laps down wins.
    expect(formatGap(31.2, 1)).toBe('+1 LAP');
  });

  it('normalises team colours that arrive without a leading hash', () => {
    expect(teamColour('3671C6')).toBe('#3671C6');
    expect(teamColour('#3671C6')).toBe('#3671C6');
    expect(teamColour(null)).toBe('#6B7483');
    expect(teamColour('not-a-colour')).toBe('#6B7483');
  });

  it('chooses a latency unit by magnitude', () => {
    expect(formatLatency(0.42)).toBe('0.42ms');
    expect(formatLatency(4.2)).toBe('4.2ms');
    expect(formatLatency(420)).toBe('420ms');
    expect(formatLatency(4200)).toBe('4.20s');
  });
});

describe('status indicators', () => {
  it('never communicates state by colour alone', () => {
    render(<HealthBadge state="down" />);
    // The word is present in the DOM, so the state survives greyscale.
    expect(screen.getByText('DOWN')).toBeInTheDocument();
  });

  it('labels a stale stream as stale rather than connected', () => {
    render(<ConnectionBadge state="stale" />);
    expect(screen.getByText('STALE')).toBeInTheDocument();
    expect(screen.queryByText('STREAM CONNECTED')).not.toBeInTheDocument();
  });

  it('renders OFFLINE when nothing is streaming', () => {
    render(<ModeBadge mode="offline" />);
    expect(screen.getByText('OFFLINE')).toBeInTheDocument();
  });
});

describe('SystemHealthPage', () => {
  const healthy: HealthResponse = {
    state: 'degraded',
    version: '0.1.0',
    environment: 'test',
    uptime_seconds: 125,
    components: [
      {
        name: 'api',
        state: 'healthy',
        detail: null,
        latency_ms: null,
        checked_at: '2026-09-20T05:00:00Z',
      },
      {
        name: 'database',
        state: 'degraded',
        detail: 'Round-trip above 250ms.',
        latency_ms: 812.4,
        checked_at: '2026-09-20T05:00:00Z',
      },
    ],
  };

  beforeEach(() => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response(JSON.stringify(healthy), { status: 200 })),
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders each component with its measured latency', async () => {
    render(
      <MemoryRouter initialEntries={['/system']}>
        <Routes>
          <Route path="/system" element={<SystemHealthPage />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => expect(screen.getByText('database')).toBeInTheDocument());
    expect(screen.getByText('812ms')).toBeInTheDocument();
    expect(screen.getByText('Round-trip above 250ms.')).toBeInTheDocument();
  });

  it('shows a dash, not a zero, for a component with no latency measurement', async () => {
    render(
      <MemoryRouter initialEntries={['/system']}>
        <Routes>
          <Route path="/system" element={<SystemHealthPage />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => expect(screen.getByText('api')).toBeInTheDocument());
    // The api card and the four unmeasured pipeline metrics all show dashes.
    expect(screen.getAllByText(NOT_AVAILABLE).length).toBeGreaterThanOrEqual(5);
  });

  it('surfaces an API failure instead of rendering empty panels', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        throw new TypeError('Failed to fetch');
      }),
    );

    render(
      <MemoryRouter initialEntries={['/system']}>
        <Routes>
          <Route path="/system" element={<SystemHealthPage />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() =>
      expect(screen.getByText('Cannot reach the API')).toBeInTheDocument(),
    );
    expect(screen.getByText('RETRY')).toBeInTheDocument();
  });
});
