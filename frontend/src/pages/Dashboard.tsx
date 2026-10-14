/**
 * / - the live dashboard.
 *
 * Information hierarchy, top to bottom and left to right, matching the order an
 * operator asks the questions: what is the session doing, where are the cars,
 * who is where, and what is the selected car doing.
 *
 * Every panel degrades independently. If position data is missing, the map says
 * so and the timing tower still works. Nothing here renders a placeholder car
 * or an invented lap time to fill space.
 */

import { useCallback, useEffect, useMemo, useState } from 'react';
import { CircuitMap } from '@/components/CircuitMap';
import { DriverPanel } from '@/components/DriverPanel';
import { TimingTower } from '@/components/TimingTower';
import { DisconnectedState, EmptyState, RetryButton } from '@/components/States';
import { SkeletonRows } from '@/components/Skeleton';
import { useLiveStream } from '@/hooks/useLiveStream';
import { api } from '@/lib/api';
import {
  NOT_AVAILABLE,
  formatAge,
  formatLapTime,
  formatTemperature,
} from '@/lib/format';
import type { DriverInfo } from '@/lib/types';

function MetricCard({
  label,
  value,
  unit,
  emphasis,
}: {
  label: string;
  value: string;
  unit?: string;
  emphasis?: boolean;
}) {
  return (
    <div className="panel flex min-w-0 flex-col gap-0.5 px-3 py-2">
      <span className="label truncate">{label}</span>
      <div className="flex items-baseline gap-1">
        <span
          className={`telemetry-value truncate ${emphasis ? 'text-xl' : 'text-base'}`}
        >
          {value}
        </span>
        {unit && <span className="telemetry-unit">{unit}</span>}
      </div>
    </div>
  );
}

export function DashboardPage() {
  const [sessionId, setSessionId] = useState<number | null>(null);
  const [selectedDriver, setSelectedDriver] = useState<number | null>(null);
  const [driverInfo, setDriverInfo] = useState<Map<number, DriverInfo>>(new Map());

  const { connection, state, lastEventAgeMs, activeSessions, error, reconnectNow } =
    useLiveStream({
      sessionId,
      channels: ['timing', 'telemetry', 'position', 'session', 'race_control'],
    });

  // Adopt whichever session is streaming, rather than hard-coding one.
  useEffect(() => {
    if (sessionId == null && activeSessions.length > 0) {
      setSessionId(activeSessions[0] ?? null);
    }
  }, [activeSessions, sessionId]);

  // Driver identity (names, teams, colours) is reference data, not stream data,
  // so it is fetched once over REST rather than pushed on every snapshot.
  useEffect(() => {
    if (sessionId == null) return;
    let cancelled = false;

    void (async () => {
      try {
        const drivers = await api.sessionDrivers(sessionId);
        if (cancelled) return;
        setDriverInfo(new Map(drivers.map((d) => [d.driver_number, d])));
      } catch {
        // Losing the entry list costs labels, not data: the tower falls back to
        // car numbers rather than failing.
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [sessionId]);

  const selected = useMemo(
    () => state?.drivers.find((d) => d.driver_number === selectedDriver) ?? null,
    [state, selectedDriver],
  );

  const handleSelect = useCallback((driverNumber: number) => {
    setSelectedDriver((current) => (current === driverNumber ? null : driverNumber));
  }, []);

  const leader = state?.drivers.find((d) => d.position === 1);

  /* ------------------------------------------------------------ empty states */

  if (connection === 'disconnected' && state == null) {
    return (
      <div className="flex h-full items-center justify-center p-4">
        <div className="panel w-full max-w-md">
          <DisconnectedState
            title="Not connected to the stream"
            detail={
              error ??
              'The dashboard is waiting for a WebSocket connection to the API.'
            }
            action={<RetryButton onClick={reconnectNow} label="RECONNECT" />}
          />
        </div>
      </div>
    );
  }

  if (state == null) {
    return (
      <div className="flex flex-col gap-3 p-4">
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 xl:grid-cols-7">
          {['SESSION', 'LAP', 'LEADER', 'FASTEST LAP', 'TRACK TEMP', 'AIR TEMP', 'CARS'].map(
            (l) => (
              <MetricCard key={l} label={l} value={NOT_AVAILABLE} />
            ),
          )}
        </div>
        <div className="panel p-3">
          <span className="label">TIMING</span>
          <div className="mt-2">
            <SkeletonRows rows={10} />
          </div>
        </div>
        <p className="text-center text-xs text-content-tertiary">
          Connected, waiting for the first events. Nothing is streaming yet.
        </p>
      </div>
    );
  }

  /* ----------------------------------------------------------------- content */

  return (
    <div className="flex flex-col gap-3 p-3">
      {/* Metric row */}
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 xl:grid-cols-7">
        <MetricCard
          label="SESSION"
          value={state.session_name ?? NOT_AVAILABLE}
        />
        <MetricCard
          label="LAP"
          value={
            state.current_lap == null
              ? NOT_AVAILABLE
              : state.total_laps
                ? `${state.current_lap}/${state.total_laps}`
                : String(state.current_lap)
          }
          emphasis
        />
        <MetricCard
          label="LEADER"
          value={
            leader
              ? (driverInfo.get(leader.driver_number)?.name_acronym ??
                 String(leader.driver_number))
              : NOT_AVAILABLE
          }
          emphasis
        />
        <MetricCard
          label="FASTEST LAP"
          value={formatLapTime(state.session_best_lap)}
        />
        <MetricCard
          label="TRACK TEMP"
          value={formatTemperature(state.weather.track_temperature)}
          unit="C"
        />
        <MetricCard
          label="AIR TEMP"
          value={formatTemperature(state.weather.air_temperature)}
          unit="C"
        />
        <MetricCard label="CARS" value={String(state.drivers.length)} />
      </div>

      {/* Map + tower */}
      <div className="grid gap-3 xl:grid-cols-[1.15fr_1fr]">
        <section className="panel flex min-h-[22rem] flex-col xl:min-h-[30rem]">
          <div className="flex items-center justify-between border-b border-line px-3 py-2">
            <span className="label">CIRCUIT</span>
            <span className="text-2xs text-content-tertiary">
              {state.circuit ?? NOT_AVAILABLE}
            </span>
          </div>
          <CircuitMap
            drivers={state.drivers}
            driverInfo={driverInfo}
            selectedDriver={selectedDriver}
            onSelectDriver={handleSelect}
            className="flex-1"
          />
        </section>

        <section className="panel flex min-h-[22rem] flex-col overflow-hidden xl:min-h-[30rem]">
          <div className="flex items-center justify-between border-b border-line px-3 py-2">
            <span className="label">LIVE TIMING</span>
            <span className="font-mono text-2xs text-content-tertiary">
              {lastEventAgeMs == null ? NOT_AVAILABLE : formatAge(lastEventAgeMs)}
            </span>
          </div>
          <div className="min-h-0 flex-1 overflow-y-auto">
            {state.drivers.length === 0 ? (
              <EmptyState
                title="No timing data"
                detail="No classification or lap events have arrived for this session yet."
              />
            ) : (
              <TimingTower
                state={state}
                driverInfo={driverInfo}
                selectedDriver={selectedDriver}
                onSelectDriver={handleSelect}
              />
            )}
          </div>
        </section>
      </div>

      {/* Selected driver */}
      <section className="panel flex min-h-[18rem] flex-col">
        <div className="flex items-center justify-between border-b border-line px-3 py-2">
          <span className="label">DRIVER TELEMETRY</span>
          {selected && (
            <button
              type="button"
              onClick={() => setSelectedDriver(null)}
              className="text-2xs font-medium uppercase tracking-label text-content-tertiary transition-colors duration-fast hover:text-content-primary"
            >
              Clear
            </button>
          )}
        </div>
        <DriverPanel
          driver={selected}
          info={selected ? driverInfo.get(selected.driver_number) : undefined}
          sessionBestSectors={state.session_best_sectors}
        />
      </section>
    </div>
  );
}
