/**
 * Loading placeholders.
 *
 * Shaped like the content they stand in for, so the layout does not jump when
 * real data arrives. A blank screen is never shown.
 */

import type { CSSProperties } from 'react';
import clsx from 'clsx';

export function Skeleton({
  className,
  style,
}: {
  className?: string;
  style?: CSSProperties;
}) {
  return <div className={clsx('skeleton', className)} style={style} aria-hidden="true" />;
}

export function SkeletonPanel({
  label,
  rows = 4,
  className,
}: {
  label: string;
  rows?: number;
  className?: string;
}) {
  return (
    <section
      className={clsx('panel p-4', className)}
      aria-busy="true"
      // The label is the accessible announcement; the bars are decoration.
      aria-label={`${label} loading`}
    >
      <span className="label">{label}</span>
      <div className="mt-3 flex flex-col gap-2">
        {Array.from({ length: rows }, (_, i) => (
          // Varying widths read as content rather than as a progress bar.
          <Skeleton key={i} className="h-3" style={{ width: `${100 - i * 8}%` }} />
        ))}
      </div>
    </section>
  );
}

/** Timing-tower placeholder: fixed row height so the tower does not resize. */
export function SkeletonRows({ rows = 8 }: { rows?: number }) {
  return (
    <div className="flex flex-col gap-px" aria-busy="true" aria-label="Timing data loading">
      {Array.from({ length: rows }, (_, i) => (
        <Skeleton key={i} className="h-7 w-full" style={{ opacity: 1 - i * 0.07 }} />
      ))}
    </div>
  );
}
