/**
 * Live timing tower.
 *
 * Performance is the design constraint. Snapshots arrive ten times a second
 * with twenty drivers, so a naive implementation re-renders four hundred rows
 * per second and drops frames on a mid-range laptop. Three things prevent that:
 *
 * 1. **Each row is its own memoised component**, compared on the fields it
 *    actually renders. A driver whose gap did not change does not re-render
 *    because another driver's did.
 * 2. **Rows are keyed by driver number, not by index**, so a position change
 *    moves a row rather than rewriting two.
 * 3. **Position-change highlights are computed from the previous snapshot in a
 *    ref**, not from state, so detecting a change does not itself cause a
 *    render.
 */

import { memo, useEffect, useRef } from 'react';
import clsx from 'clsx';
import {
  NOT_AVAILABLE,
  formatGap,
  formatInt,
  formatLapTime,
  formatSector,
  teamColour,
} from '@/lib/format';
import type { DriverInfo, LiveDriver, LiveSessionState } from '@/lib/types';

type PositionDelta = 'gained' | 'lost' | null;

interface RowProps {
  driver: LiveDriver;
  info: DriverInfo | undefined;
  isSelected: boolean;
  isSessionBestLap: boolean;
  sessionBestSectors: [number | null, number | null, number | null];
  delta: PositionDelta;
  onSelect: (driverNumber: number) => void;
}

/** One sector cell, coloured by how it compares. Colour is never the only cue. */
function SectorCell({
  value,
  personalBest,
  sessionBest,
}: {
  value: number | null;
  personalBest: number | null;
  sessionBest: number | null;
}) {
  // A sector with no time yet is blank, not zero.
  if (value == null) {
    return <span className="telemetry-value text-xs text-content-disabled">{NOT_AVAILABLE}</span>;
  }

  const isSessionBest = sessionBest != null && value <= sessionBest;
  const isPersonalBest = !isSessionBest && personalBest != null && value <= personalBest;

  return (
    <span
      className={clsx(
        'telemetry-value text-xs',
        isSessionBest && 'text-[#C77DFF]',   // purple: session best, the broadcast convention
        isPersonalBest && 'text-status-healthy',
        !isSessionBest && !isPersonalBest && 'text-content-secondary',
      )}
      // The colour convention is not universally known, so the meaning is also
      // available to a screen reader and on hover.
      title={
        isSessionBest ? 'Session best' : isPersonalBest ? 'Personal best' : undefined
      }
    >
      {formatSector(value)}
      {isSessionBest && <span className="sr-only"> (session best)</span>}
      {isPersonalBest && <span className="sr-only"> (personal best)</span>}
    </span>
  );
}

const TimingRow = memo(
  function TimingRow({
    driver,
    info,
    isSelected,
    isSessionBestLap,
    sessionBestSectors,
    delta,
    onSelect,
  }: RowProps) {
    const colour = teamColour(info?.team_colour);
    const acronym = info?.name_acronym ?? String(driver.driver_number);

    return (
      <tr
        onClick={() => onSelect(driver.driver_number)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            onSelect(driver.driver_number);
          }
        }}
        tabIndex={0}
        aria-selected={isSelected}
        className={clsx(
          'group cursor-pointer border-b border-line-subtle transition-colors duration-fast',
          isSelected ? 'bg-surface-inset' : 'hover:bg-surface-hover',
          delta === 'gained' && 'animate-position-gain',
          delta === 'lost' && 'animate-position-loss',
        )}
      >
        {/* Position, with the team colour as a left edge. */}
        <td className="relative py-1.5 pl-3 pr-2">
          <span
            aria-hidden="true"
            className="absolute inset-y-0 left-0 w-0.5"
            style={{ backgroundColor: colour }}
          />
          <span className="telemetry-value text-sm font-semibold">
            {driver.position ?? NOT_AVAILABLE}
          </span>
          {delta && (
            <span
              className={clsx(
                'ml-1 text-2xs',
                delta === 'gained' ? 'text-status-healthy' : 'text-status-warning',
              )}
              aria-label={delta === 'gained' ? 'Position gained' : 'Position lost'}
            >
              {delta === 'gained' ? '▲' : '▼'}
            </span>
          )}
        </td>

        <td className="px-2 py-1.5">
          <div className="flex items-baseline gap-1.5">
            <span className="text-xs font-semibold text-content-primary">{acronym}</span>
            <span className="telemetry-value text-2xs text-content-tertiary">
              {driver.driver_number}
            </span>
          </div>
        </td>

        <td className="hidden px-2 py-1.5 lg:table-cell">
          <span className="truncate text-2xs text-content-tertiary">
            {info?.team_name ?? NOT_AVAILABLE}
          </span>
        </td>

        <td className="px-2 py-1.5 text-right">
          <span className="telemetry-value text-xs text-content-secondary">
            {driver.current_lap ?? NOT_AVAILABLE}
          </span>
        </td>

        <td className="px-2 py-1.5 text-right">
          <span className="telemetry-value text-xs text-content-secondary">
            {formatGap(driver.gap_to_leader, driver.laps_down)}
          </span>
        </td>

        <td className="px-2 py-1.5 text-right">
          <span
            className={clsx(
              'telemetry-value text-xs',
              driver.last_lap_was_personal_best
                ? 'text-status-healthy'
                : 'text-content-primary',
            )}
          >
            {formatLapTime(driver.last_lap_time)}
          </span>
        </td>

        <td className="px-2 py-1.5 text-right">
          <span
            className={clsx(
              'telemetry-value text-xs',
              isSessionBestLap ? 'text-[#C77DFF]' : 'text-content-secondary',
            )}
            title={isSessionBestLap ? 'Session best lap' : undefined}
          >
            {formatLapTime(driver.best_lap_time)}
            {isSessionBestLap && <span className="sr-only"> (session best lap)</span>}
          </span>
        </td>

        {[0, 1, 2].map((i) => (
          <td key={i} className="hidden px-2 py-1.5 text-right md:table-cell">
            <SectorCell
              value={driver.sectors.current[i] ?? null}
              personalBest={driver.sectors.best[i] ?? null}
              sessionBest={sessionBestSectors[i] ?? null}
            />
          </td>
        ))}

        <td className="px-2 py-1.5 pr-3 text-right">
          <span className="telemetry-value text-xs text-content-secondary">
            {formatInt(driver.telemetry.speed)}
          </span>
        </td>
      </tr>
    );
  },
  // Only re-render when something this row actually displays has changed.
  (prev, next) =>
    prev.driver.position === next.driver.position &&
    prev.driver.gap_to_leader === next.driver.gap_to_leader &&
    prev.driver.laps_down === next.driver.laps_down &&
    prev.driver.current_lap === next.driver.current_lap &&
    prev.driver.last_lap_time === next.driver.last_lap_time &&
    prev.driver.best_lap_time === next.driver.best_lap_time &&
    prev.driver.telemetry.speed === next.driver.telemetry.speed &&
    prev.driver.sectors.current[0] === next.driver.sectors.current[0] &&
    prev.driver.sectors.current[1] === next.driver.sectors.current[1] &&
    prev.driver.sectors.current[2] === next.driver.sectors.current[2] &&
    prev.driver.last_lap_was_personal_best === next.driver.last_lap_was_personal_best &&
    prev.isSelected === next.isSelected &&
    prev.isSessionBestLap === next.isSessionBestLap &&
    prev.delta === next.delta &&
    prev.info === next.info,
);

interface TimingTowerProps {
  state: LiveSessionState | null;
  driverInfo: Map<number, DriverInfo>;
  selectedDriver: number | null;
  onSelectDriver: (driverNumber: number) => void;
}

const COLUMNS = [
  { label: 'POS', className: 'pl-3 pr-2 text-left' },
  { label: 'DRIVER', className: 'px-2 text-left' },
  { label: 'TEAM', className: 'px-2 text-left hidden lg:table-cell' },
  { label: 'LAP', className: 'px-2 text-right' },
  { label: 'GAP', className: 'px-2 text-right' },
  { label: 'LAST', className: 'px-2 text-right' },
  { label: 'BEST', className: 'px-2 text-right' },
  { label: 'S1', className: 'px-2 text-right hidden md:table-cell' },
  { label: 'S2', className: 'px-2 text-right hidden md:table-cell' },
  { label: 'S3', className: 'px-2 text-right hidden md:table-cell' },
  { label: 'SPEED', className: 'px-2 pr-3 text-right' },
];

export function TimingTower({
  state,
  driverInfo,
  selectedDriver,
  onSelectDriver,
}: TimingTowerProps) {
  // Previous positions, held in a ref so that detecting a change does not
  // itself trigger a render.
  const previousPositions = useRef<Map<number, number>>(new Map());
  const deltas = useRef<Map<number, PositionDelta>>(new Map());

  const drivers = state?.drivers ?? [];

  useEffect(() => {
    const next = new Map<number, number>();
    const nextDeltas = new Map<number, PositionDelta>();

    for (const d of drivers) {
      if (d.position == null) continue;
      next.set(d.driver_number, d.position);
      const before = previousPositions.current.get(d.driver_number);
      if (before != null && before !== d.position) {
        nextDeltas.set(d.driver_number, d.position < before ? 'gained' : 'lost');
      }
    }

    deltas.current = nextDeltas;
    previousPositions.current = next;

    // Clear the highlight after the animation so a change reads as a moment,
    // not a permanent state.
    if (nextDeltas.size > 0) {
      const timer = setTimeout(() => {
        deltas.current = new Map();
      }, 1000);
      return () => clearTimeout(timer);
    }
    return undefined;
  }, [drivers]);

  return (
    <div className="flex min-h-0 flex-col">
      <table className="w-full border-collapse">
        <caption className="sr-only">
          Live timing. Select a driver to view their telemetry.
        </caption>
        <thead className="sticky top-0 z-10 bg-surface-inset">
          <tr className="border-b border-line">
            {COLUMNS.map((col) => (
              <th
                key={col.label}
                scope="col"
                className={clsx(
                  'py-1.5 text-2xs font-medium uppercase tracking-label text-content-tertiary',
                  col.className,
                )}
              >
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {drivers.map((driver) => (
            <TimingRow
              // Keyed by driver, so a position change moves a row rather than
              // rewriting two.
              key={driver.driver_number}
              driver={driver}
              info={driverInfo.get(driver.driver_number)}
              isSelected={selectedDriver === driver.driver_number}
              isSessionBestLap={
                state?.session_best_lap_driver === driver.driver_number &&
                driver.best_lap_time != null
              }
              sessionBestSectors={state?.session_best_sectors ?? [null, null, null]}
              delta={deltas.current.get(driver.driver_number) ?? null}
              onSelect={onSelectDriver}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}
