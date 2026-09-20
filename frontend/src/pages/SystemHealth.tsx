/**
 * /system - System Health.
 *
 * Every figure on this page comes from the API's own measurements. A component
 * that could not be measured shows an em-dash, never a zero. The page never
 * fabricates a reading to fill a card.
 */

import { HealthBadge } from '@/components/StatusIndicator';
import { ErrorState, RetryButton } from '@/components/States';
import { SkeletonPanel } from '@/components/Skeleton';
import { useHealth } from '@/hooks/useHealth';
import { formatClock, formatDuration, formatLatency, NOT_AVAILABLE } from '@/lib/format';
import type { ComponentHealth } from '@/lib/types';

function ComponentCard({ component }: { component: ComponentHealth }) {
  return (
    <article className="panel flex flex-col gap-3 p-4">
      <div className="flex items-start justify-between gap-3">
        <h3 className="text-xs font-semibold uppercase tracking-label text-content-primary">
          {component.name}
        </h3>
        <HealthBadge state={component.state} />
      </div>

      <div className="flex items-baseline gap-1.5">
        <span className="telemetry-value text-2xl">
          {formatLatency(component.latency_ms)}
        </span>
        <span className="telemetry-unit">RTT</span>
      </div>

      {/* The reason a component is unhealthy is the most useful thing on the
       * card, so it is shown rather than hidden behind a tooltip. */}
      <p className="min-h-[2rem] text-xs leading-relaxed text-content-tertiary">
        {component.detail ??
          (component.latency_ms == null
            ? 'No round-trip measurement is taken for this component.'
            : 'Operating normally.')}
      </p>

      <div className="flex items-center justify-between border-t border-line-subtle pt-2">
        <span className="label">CHECKED</span>
        <span className="font-mono text-2xs text-content-tertiary">
          {formatClock(component.checked_at)} UTC
        </span>
      </div>
    </article>
  );
}

export function SystemHealthPage() {
  const { state, refetch } = useHealth();

  return (
    <div className="flex flex-col gap-4 p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-content-primary">System Health</h2>
          <p className="mt-0.5 text-xs text-content-tertiary">
            Live component status from the API. Values are measured, not estimated.
          </p>
        </div>

        {state.status === 'success' && (
          <div className="flex items-center gap-4">
            <div className="flex flex-col items-end">
              <span className="label">API UPTIME</span>
              <span className="telemetry-value text-xs">
                {formatDuration(state.data.uptime_seconds)}
              </span>
            </div>
            <div className="flex flex-col items-end">
              <span className="label">VERSION</span>
              <span className="telemetry-value text-xs">{state.data.version}</span>
            </div>
            <HealthBadge state={state.data.state} />
          </div>
        )}
      </div>

      {state.status === 'loading' || state.status === 'idle' ? (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <SkeletonPanel label="API" rows={3} />
          <SkeletonPanel label="DATABASE" rows={3} />
          <SkeletonPanel label="STREAMING" rows={3} />
        </div>
      ) : state.status === 'error' ? (
        <div className="panel">
          <ErrorState
            title="Cannot reach the API"
            detail={state.error}
            action={<RetryButton onClick={() => void refetch()} />}
          />
        </div>
      ) : state.status === 'success' ? (
        <>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {state.data.components.map((component) => (
              <ComponentCard key={component.name} component={component} />
            ))}
          </div>

          {/*
            Throughput, consumer lag and end-to-end latency are deliberately
            absent rather than shown as zeroes: nothing is streaming yet, so
            there is nothing measured to report. They arrive with the pipeline
            metrics in Phase 8.
          */}
          <section className="panel p-4">
            <h3 className="label">PIPELINE METRICS</h3>
            <div className="mt-3 grid grid-cols-2 gap-4 sm:grid-cols-4">
              {['MESSAGES/SEC', 'CONSUMER LAG', 'P95 LATENCY', 'ERROR RATE'].map((label) => (
                <div key={label} className="flex flex-col gap-1">
                  <span className="label">{label}</span>
                  <span className="telemetry-value text-xl text-content-disabled">
                    {NOT_AVAILABLE}
                  </span>
                </div>
              ))}
            </div>
            <p className="mt-3 border-t border-line-subtle pt-2 text-xs text-content-tertiary">
              No pipeline traffic has been observed yet, so these have no measured
              value to report.
            </p>
          </section>
        </>
      ) : null}
    </div>
  );
}
