/**
 * Selected-driver telemetry.
 *
 * Shows the current value of each channel plus a short rolling trace. The trace
 * buffer is fixed-size and lives in a ref: a race session is millions of
 * samples, and the browser only ever needs the window it is drawing.
 */

import { memo, useEffect, useRef, useState } from 'react';
import clsx from 'clsx';
import { config } from '@/lib/config';
import {
  NOT_AVAILABLE,
  formatGap,
  formatInt,
  formatLapTime,
  formatSector,
  teamColour,
} from '@/lib/format';
import type { DriverInfo, LiveDriver } from '@/lib/types';

/** A bar that represents a 0-100 channel. */
function ChannelBar({
  label,
  value,
  colour,
}: {
  label: string;
  value: number | null;
  colour: string;
}) {
  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-baseline justify-between">
        <span className="label">{label}</span>
        <span className="telemetry-value text-xs">
          {value == null ? NOT_AVAILABLE : `${Math.round(value)}%`}
        </span>
      </div>
      <div
        className="h-1 w-full overflow-hidden rounded-full bg-surface-inset"
        role="meter"
        aria-label={label}
        aria-valuenow={value ?? undefined}
        aria-valuemin={0}
        aria-valuemax={100}
        // A channel with no reading shows an empty track, and the text above it
        // says so. It is not rendered as 0%.
        aria-valuetext={value == null ? 'No reading' : `${Math.round(value)} percent`}
      >
        <div
          className="h-full rounded-full transition-[width] duration-100"
          style={{ width: `${value ?? 0}%`, backgroundColor: colour }}
        />
      </div>
    </div>
  );
}

/** A compact sparkline over the recent window. */
function Sparkline({
  points,
  max,
  colour,
  label,
}: {
  points: number[];
  max: number;
  colour: string;
  label: string;
}) {
  if (points.length < 2) {
    return (
      <div className="flex h-10 items-center justify-center">
        <span className="text-2xs text-content-disabled">{NOT_AVAILABLE}</span>
      </div>
    );
  }

  const width = 100;
  const height = 28;
  const step = width / (points.length - 1);
  const d = points
    .map((v, i) => {
      const x = (i * step).toFixed(2);
      const y = (height - Math.min(1, v / max) * height).toFixed(2);
      return `${i === 0 ? 'M' : 'L'}${x},${y}`;
    })
    .join('');

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      className="h-10 w-full"
      preserveAspectRatio="none"
      role="img"
      aria-label={`${label} over the last ${config.telemetryWindowSeconds} seconds`}
    >
      <path d={d} fill="none" stroke={colour} strokeWidth="1.2" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

interface DriverPanelProps {
  driver: LiveDriver | null;
  info: DriverInfo | undefined;
  sessionBestSectors: [number | null, number | null, number | null];
}

export const DriverPanel = memo(function DriverPanel({
  driver,
  info,
  sessionBestSectors,
}: DriverPanelProps) {
  // Rolling buffers, held in a ref and published on a timer. Writing them to
  // state on every snapshot would re-render this panel ten times a second.
  const buffers = useRef<{ speed: number[]; rpm: number[]; throttle: number[] }>({
    speed: [],
    rpm: [],
    throttle: [],
  });
  const [, setTick] = useState(0);
  const currentDriver = useRef<number | null>(null);

  useEffect(() => {
    if (!driver) return;

    // A different driver means a different trace; keeping the old samples
    // would draw one car's history under another car's label.
    if (currentDriver.current !== driver.driver_number) {
      buffers.current = { speed: [], rpm: [], throttle: [] };
      currentDriver.current = driver.driver_number;
    }

    const push = (key: keyof typeof buffers.current, value: number | null) => {
      if (value == null) return;
      const buffer = buffers.current[key];
      buffer.push(value);
      if (buffer.length > config.maxSamplesPerChannel) buffer.shift();
    };

    push('speed', driver.telemetry.speed);
    push('rpm', driver.telemetry.rpm);
    push('throttle', driver.telemetry.throttle);
  }, [driver]);

  useEffect(() => {
    const timer = setInterval(() => setTick((t) => t + 1), config.renderIntervalMs * 2);
    return () => clearInterval(timer);
  }, []);

  if (!driver) {
    return (
      <div className="flex flex-1 flex-col items-center justify-center gap-2 p-6 text-center">
        <p className="text-sm font-medium text-content-secondary">No driver selected</p>
        <p className="max-w-xs text-xs text-content-tertiary">
          Select a driver from the timing tower or the circuit map.
        </p>
      </div>
    );
  }

  const colour = teamColour(info?.team_colour);
  const drsOpen =
    driver.telemetry.drs == null
      ? null
      : [10, 12, 14].includes(driver.telemetry.drs)
        ? true
        : [0, 1, 8].includes(driver.telemetry.drs)
          ? false
          : null;

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3 p-3">
      {/* Identity */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2.5">
          <span
            className="h-7 w-0.5 rounded-full"
            style={{ backgroundColor: colour }}
            aria-hidden="true"
          />
          <div>
            <div className="flex items-baseline gap-2">
              <span className="text-sm font-semibold text-content-primary">
                {info?.name_acronym ?? driver.driver_number}
              </span>
              <span className="telemetry-value text-2xs text-content-tertiary">
                #{driver.driver_number}
              </span>
            </div>
            <span className="text-2xs text-content-tertiary">
              {info?.team_name ?? NOT_AVAILABLE}
            </span>
          </div>
        </div>
        <div className="flex flex-col items-end">
          <span className="label">POS</span>
          <span className="telemetry-value text-xl font-semibold">
            {driver.position ?? NOT_AVAILABLE}
          </span>
        </div>
      </div>

      {/* Speed, the headline number */}
      <div className="panel-inset flex items-baseline justify-between px-3 py-2">
        <div className="flex items-baseline gap-1.5">
          <span className="telemetry-value text-hero leading-none">
            {formatInt(driver.telemetry.speed)}
          </span>
          <span className="telemetry-unit">km/h</span>
        </div>
        <div className="flex flex-col items-end gap-0.5">
          <span className="label">GEAR</span>
          <span className="telemetry-value text-lg">
            {driver.telemetry.n_gear ?? NOT_AVAILABLE}
          </span>
        </div>
        <div className="flex flex-col items-end gap-0.5">
          <span className="label">RPM</span>
          <span className="telemetry-value text-sm">{formatInt(driver.telemetry.rpm)}</span>
        </div>
        <div className="flex flex-col items-end gap-0.5">
          <span className="label">DRS</span>
          <span
            className={clsx(
              'telemetry-value text-sm',
              drsOpen === true && 'text-status-healthy',
              drsOpen === false && 'text-content-tertiary',
              drsOpen === null && 'text-content-disabled',
            )}
          >
            {/* An undocumented DRS code is reported as unknown, not guessed. */}
            {drsOpen === null ? NOT_AVAILABLE : drsOpen ? 'OPEN' : 'OFF'}
          </span>
        </div>
      </div>

      {/* Pedals */}
      <div className="grid grid-cols-2 gap-3">
        <ChannelBar label="THROTTLE" value={driver.telemetry.throttle} colour="#00D48A" />
        <ChannelBar label="BRAKE" value={driver.telemetry.brake} colour="#FF4757" />
      </div>

      {/* Traces */}
      <div className="grid grid-cols-3 gap-2">
        {([
          ['SPEED', buffers.current.speed, 360, '#3B9EFF'],
          ['RPM', buffers.current.rpm, 15000, '#FFB020'],
          ['THROTTLE', buffers.current.throttle, 100, '#00D48A'],
        ] as const).map(([label, points, max, lineColour]) => (
          <div key={label} className="panel-inset flex flex-col gap-1 p-2">
            <span className="label">{label}</span>
            <Sparkline points={points} max={max} colour={lineColour} label={label} />
          </div>
        ))}
      </div>

      {/* Lap and sector performance */}
      <div className="panel-inset flex flex-col gap-2 p-3">
        <div className="grid grid-cols-3 gap-2">
          {[
            ['LAP', driver.current_lap ?? NOT_AVAILABLE],
            ['LAST LAP', formatLapTime(driver.last_lap_time)],
            ['BEST LAP', formatLapTime(driver.best_lap_time)],
          ].map(([label, value]) => (
            <div key={label} className="flex flex-col gap-0.5">
              <span className="label">{label}</span>
              <span className="telemetry-value text-sm">{value}</span>
            </div>
          ))}
        </div>

        <div className="divider-h" />

        <div className="grid grid-cols-3 gap-2">
          {[0, 1, 2].map((i) => {
            const current = driver.sectors.current[i] ?? null;
            const best = driver.sectors.best[i] ?? null;
            const sessionBest = sessionBestSectors[i] ?? null;
            const isSessionBest = current != null && sessionBest != null && current <= sessionBest;
            const isPersonalBest = !isSessionBest && current != null && best != null && current <= best;

            return (
              <div key={i} className="flex flex-col gap-0.5">
                <span className="label">S{i + 1}</span>
                <span
                  className={clsx(
                    'telemetry-value text-sm',
                    isSessionBest && 'text-[#C77DFF]',
                    isPersonalBest && 'text-status-healthy',
                  )}
                >
                  {formatSector(current)}
                </span>
                <span className="telemetry-value text-2xs text-content-tertiary">
                  {formatSector(best)}
                </span>
              </div>
            );
          })}
        </div>

        <div className="divider-h" />

        <div className="flex items-baseline justify-between">
          <span className="label">GAP TO LEADER</span>
          <span className="telemetry-value text-sm">
            {formatGap(driver.gap_to_leader, driver.laps_down)}
          </span>
        </div>
      </div>
    </div>
  );
});
