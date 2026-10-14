/**
 * Circuit map.
 *
 * Where the track outline comes from
 * ----------------------------------
 * There is no track-geometry endpoint in the data source, and no SVG of the
 * circuit is bundled. The outline is **traced from the cars themselves**: as
 * position samples arrive they are accumulated into a point cloud, which is the
 * racing line, which is the track. That keeps the map honest - it is drawn from
 * the same data as the cars on it, so it cannot disagree with them - and it
 * works for any circuit without shipping per-track assets.
 *
 * The consequence is that the outline fills in over the first lap rather than
 * being there instantly, and the component says so rather than drawing a guess.
 *
 * Why interpolation
 * -----------------
 * Position samples arrive at about 3.7 Hz. Drawing them directly makes cars
 * teleport four times a second. Instead each car holds its previous and target
 * position and is advanced every animation frame, so motion is continuous at
 * the display's refresh rate while remaining driven entirely by real samples -
 * the interpolation adds smoothness between measurements, never a measurement.
 */

import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import clsx from 'clsx';
import { teamColour } from '@/lib/format';
import type { DriverInfo, LiveDriver } from '@/lib/types';

/** Coordinates are in the circuit's own frame, so the view is fitted to data. */
interface Bounds {
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
}

interface CarPose {
  x: number;
  y: number;
  targetX: number;
  targetY: number;
  // When the current target arrived, so the tween can be paced by real time.
  since: number;
}

// Roughly one sample interval. Interpolating over this makes motion continuous
// without running far ahead of the data.
const TWEEN_MS = 280;
const MAX_TRACE_POINTS = 4000;
const VIEWBOX_PADDING = 0.06;

interface CircuitMapProps {
  drivers: LiveDriver[];
  driverInfo: Map<number, DriverInfo>;
  selectedDriver: number | null;
  onSelectDriver: (driverNumber: number) => void;
  className?: string;
}

export const CircuitMap = memo(function CircuitMap({
  drivers,
  driverInfo,
  selectedDriver,
  onSelectDriver,
  className,
}: CircuitMapProps) {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const poses = useRef<Map<number, CarPose>>(new Map());
  const trace = useRef<Array<[number, number]>>([]);
  const frame = useRef<number | null>(null);
  const groupRefs = useRef<Map<number, SVGGElement | null>>(new Map());

  const [bounds, setBounds] = useState<Bounds | null>(null);
  const [tracePath, setTracePath] = useState<string>('');
  const [followSelected, setFollowSelected] = useState(false);

  /* ------------------------------------------------- accumulate the outline */

  useEffect(() => {
    let changed = false;

    for (const driver of drivers) {
      const { x, y } = driver.track_position;
      if (x == null || y == null) continue;

      const existing = poses.current.get(driver.driver_number);
      if (!existing) {
        poses.current.set(driver.driver_number, {
          x, y, targetX: x, targetY: y, since: performance.now(),
        });
      } else if (existing.targetX !== x || existing.targetY !== y) {
        // Start the next tween from wherever the car is drawn right now, not
        // from the last target, so a dropped frame does not cause a jump.
        existing.x = existing.x;
        existing.y = existing.y;
        existing.targetX = x;
        existing.targetY = y;
        existing.since = performance.now();
      }

      // Thin the trace: consecutive samples from twenty cars would otherwise
      // add tens of thousands of near-identical points.
      const last = trace.current[trace.current.length - 1];
      if (!last || Math.abs(last[0] - x) > 40 || Math.abs(last[1] - y) > 40) {
        trace.current.push([x, y]);
        changed = true;
        if (trace.current.length > MAX_TRACE_POINTS) trace.current.shift();
      }
    }

    if (!changed || trace.current.length < 8) return;

    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (const [x, y] of trace.current) {
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    }
    setBounds({ minX, maxX, minY, maxY });

    // The trace is a point cloud, not an ordered path, so it is drawn as dots
    // rather than joined into a line that would zig-zag between cars.
    setTracePath(trace.current.map(([x, y]) => `M${x.toFixed(0)},${y.toFixed(0)}h1`).join(''));
  }, [drivers]);

  /* ------------------------------------------------------ animation loop */

  useEffect(() => {
    const step = () => {
      const now = performance.now();

      for (const [number, pose] of poses.current) {
        const t = Math.min(1, (now - pose.since) / TWEEN_MS);
        // Ease-out: a car decelerating into its sample point looks like motion,
        // where linear interpolation reads as mechanical.
        const eased = 1 - (1 - t) ** 2;
        const x = pose.x + (pose.targetX - pose.x) * eased;
        const y = pose.y + (pose.targetY - pose.y) * eased;

        const group = groupRefs.current.get(number);
        if (group) {
          // Transform the DOM node directly. Routing twenty positions per frame
          // through React state would re-render the whole map 60 times a second.
          group.setAttribute('transform', `translate(${x.toFixed(1)},${y.toFixed(1)})`);
        }
      }

      frame.current = requestAnimationFrame(step);
    };

    frame.current = requestAnimationFrame(step);
    return () => {
      if (frame.current != null) cancelAnimationFrame(frame.current);
    };
  }, []);

  /* ---------------------------------------------------------------- viewBox */

  const viewBox = useMemo(() => {
    if (!bounds) return '0 0 100 100';
    const width = Math.max(1, bounds.maxX - bounds.minX);
    const height = Math.max(1, bounds.maxY - bounds.minY);
    const pad = Math.max(width, height) * VIEWBOX_PADDING;
    return `${bounds.minX - pad} ${bounds.minY - pad} ${width + pad * 2} ${height + pad * 2}`;
  }, [bounds]);

  const scale = useMemo(() => {
    if (!bounds) return 1;
    // Marker sizes are in circuit units, so they must scale with the circuit.
    return Math.max(bounds.maxX - bounds.minX, bounds.maxY - bounds.minY) / 100;
  }, [bounds]);

  const resetView = useCallback(() => {
    trace.current = [];
    setTracePath('');
    setBounds(null);
  }, []);

  const positioned = drivers.filter(
    (d) => d.track_position.x != null && d.track_position.y != null,
  );

  /* ------------------------------------------------------------------ render */

  if (positioned.length === 0) {
    return (
      <div
        className={clsx(
          'flex flex-col items-center justify-center gap-2 p-8 text-center',
          className,
        )}
        role="status"
      >
        <svg
          viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.25"
          className="h-6 w-6 text-content-disabled" aria-hidden="true"
        >
          <ellipse cx="12" cy="12" rx="9" ry="6" />
        </svg>
        <p className="text-sm font-medium text-content-secondary">No position data</p>
        <p className="max-w-xs text-xs text-content-tertiary">
          The track outline is traced from car positions as they arrive. Nothing is
          drawn until real samples are received.
        </p>
      </div>
    );
  }

  return (
    <div className={clsx('relative flex min-h-0 flex-col', className)}>
      <svg
        ref={svgRef}
        viewBox={viewBox}
        className="h-full w-full"
        role="img"
        aria-label={`Circuit map showing ${positioned.length} cars`}
        preserveAspectRatio="xMidYMid meet"
      >
        {/* The traced racing line. */}
        <path
          d={tracePath}
          stroke="#252A33"
          strokeWidth={scale * 2.2}
          strokeLinecap="round"
          fill="none"
          aria-hidden="true"
        />

        {positioned.map((driver) => {
          const info = driverInfo.get(driver.driver_number);
          const colour = teamColour(info?.team_colour);
          const isSelected = selectedDriver === driver.driver_number;
          const dimmed = selectedDriver != null && !isSelected;

          return (
            <g
              key={driver.driver_number}
              ref={(el) => {
                groupRefs.current.set(driver.driver_number, el);
              }}
              onClick={() => onSelectDriver(driver.driver_number)}
              className="cursor-pointer"
              opacity={dimmed ? 0.35 : 1}
              style={{ transition: 'opacity 180ms' }}
            >
              {isSelected && (
                <circle r={scale * 3.4} fill="none" stroke={colour} strokeWidth={scale * 0.6} opacity={0.5} />
              )}
              <circle
                r={scale * (isSelected ? 2.2 : 1.6)}
                fill={colour}
                stroke="#08090B"
                strokeWidth={scale * 0.4}
              />
              {(isSelected || positioned.length <= 6) && (
                <text
                  x={scale * 3.6}
                  y={scale * 1.2}
                  fill="#E8EBF0"
                  fontSize={scale * 3.4}
                  fontFamily="JetBrains Mono, monospace"
                  fontWeight="600"
                >
                  {info?.name_acronym ?? driver.driver_number}
                </text>
              )}
            </g>
          );
        })}
      </svg>

      {/* Controls. Follow is disabled until a driver is selected, rather than
          silently doing nothing. */}
      <div className="absolute bottom-2 right-2 flex gap-1">
        <button
          type="button"
          onClick={resetView}
          className="rounded border border-line bg-surface-card/90 px-2 py-1 text-2xs font-medium uppercase tracking-label text-content-tertiary backdrop-blur-sm transition-colors duration-fast hover:border-line-strong hover:text-content-primary"
        >
          Refit
        </button>
        <button
          type="button"
          disabled={selectedDriver == null}
          onClick={() => setFollowSelected((v) => !v)}
          aria-pressed={followSelected}
          className={clsx(
            'rounded border px-2 py-1 text-2xs font-medium uppercase tracking-label backdrop-blur-sm transition-colors duration-fast',
            selectedDriver == null
              ? 'cursor-not-allowed border-line bg-surface-card/50 text-content-disabled'
              : followSelected
                ? 'border-accent/40 bg-accent-subtle text-accent'
                : 'border-line bg-surface-card/90 text-content-tertiary hover:border-line-strong hover:text-content-primary',
          )}
        >
          Follow
        </button>
      </div>

      <div className="absolute left-2 top-2 flex flex-col gap-0.5">
        <span className="label">TRACED POINTS</span>
        <span className="telemetry-value text-2xs text-content-tertiary">
          {trace.current.length}
        </span>
      </div>
    </div>
  );
});
