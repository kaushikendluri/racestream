/**
 * Honest placeholders for routes whose feature has not been built yet.
 *
 * These exist so navigation works end to end from Phase 1, and they say plainly
 * what is missing and which phase delivers it. They deliberately do not render a
 * mock dashboard: a screenshot of invented telemetry is exactly the thing this
 * project claims not to do.
 */

import { Link } from 'react-router-dom';

interface PlaceholderProps {
  title: string;
  phase: string;
  summary: string;
  /** What this screen will show once implemented. */
  planned: string[];
}

export function PhasePlaceholder({ title, phase, summary, planned }: PlaceholderProps) {
  return (
    <div className="flex flex-col gap-4 p-4">
      <div>
        <div className="flex items-center gap-2.5">
          <h2 className="text-lg font-semibold text-content-primary">{title}</h2>
          <span className="rounded-sm border border-line bg-surface-inset px-1.5 py-0.5 text-2xs font-medium uppercase tracking-label text-content-tertiary">
            {phase}
          </span>
        </div>
        <p className="mt-0.5 text-xs text-content-tertiary">{summary}</p>
      </div>

      <section className="panel p-5">
        <h3 className="label">NOT YET IMPLEMENTED</h3>
        <p className="mt-2 max-w-xl text-sm leading-relaxed text-content-secondary">
          This screen has no data to show yet. Rather than render a mock-up, it
          stays empty until the pipeline behind it is real.
        </p>

        <h4 className="label mt-5">PLANNED</h4>
        <ul className="mt-2 flex flex-col gap-1.5" role="list">
          {planned.map((item) => (
            <li key={item} className="flex items-start gap-2 text-xs text-content-tertiary">
              <span
                className="mt-1.5 h-1 w-1 shrink-0 rounded-full bg-content-disabled"
                aria-hidden="true"
              />
              {item}
            </li>
          ))}
        </ul>

        <div className="mt-5 border-t border-line-subtle pt-4">
          <Link
            to="/system"
            className="text-xs font-medium text-status-info transition-colors duration-fast hover:text-content-primary"
          >
            System Health is live now &rarr;
          </Link>
        </div>
      </section>
    </div>
  );
}

export const DashboardPage = () => (
  <PhasePlaceholder
    title="Live Dashboard"
    phase="PHASE 6"
    summary="Session state, live timing, circuit map and selected-driver telemetry."
    planned={[
      'Metric row: current lap, session time, leader, fastest lap, track and air temperature',
      'Live timing tower driven by WebSocket updates, with per-cell diffing rather than full re-render',
      'Interactive circuit map with interpolated car positions',
      'Speed, throttle, brake, RPM and gear traces on a synchronised time axis',
      'Sector analysis with driver-to-driver comparison',
    ]}
  />
);

export const SessionsPage = () => (
  <PhasePlaceholder
    title="Session Explorer"
    phase="PHASE 2"
    summary="Browse ingested sessions and inspect what telemetry each one carries."
    planned={[
      'Season, Grand Prix, session-type and date filters',
      'Per-session availability of each telemetry channel, read from the database',
      'Ingest status and replay availability',
      'Session detail: classification, drivers, stints, weather and race-control events',
    ]}
  />
);

export const ReplayPage = () => (
  <PhasePlaceholder
    title="Replay Control"
    phase="PHASE 7"
    summary="Replay a historical session through the same streaming pipeline as live data."
    planned={[
      'Play, pause, stop and seek against a replay clock',
      'Speed multipliers from 0.5x to 10x with event timing preserved',
      'Timeline markers for lap starts, pit stops, safety cars, red flags and race control',
      'Replay publishes to the same topics and is read by the same consumer groups as live',
    ]}
  />
);

export const AnalyticsPage = () => (
  <PhasePlaceholder
    title="Telemetry Analytics"
    phase="PHASE 6"
    summary="Post-session analysis over data already in TimescaleDB."
    planned={[
      'Lap-time evolution and stint analysis',
      'Speed traces and sector comparison between drivers',
      'Tyre compound usage and track temperature over a session',
      'Position evolution across the race',
    ]}
  />
);

export const DriverPage = () => (
  <PhasePlaceholder
    title="Driver Telemetry"
    phase="PHASE 6"
    summary="Per-driver telemetry, lap and sector performance for a selected session."
    planned={[
      'Live channel readout: speed, throttle, brake, gear, RPM and DRS',
      'Lap times with personal and session bests marked',
      'Sector performance and position changes',
      'Stint history with compound and tyre age',
    ]}
  />
);

export const SettingsPage = () => (
  <PhasePlaceholder
    title="Settings"
    phase="PHASE 6"
    summary="Display preferences and stream thresholds."
    planned={[
      'Data-freshness thresholds for the STALE and DISCONNECTED transitions',
      'Units and time-zone display',
      'Telemetry buffer window',
      'Reduced-motion override',
    ]}
  />
);
