/**
 * UTC wall clock.
 *
 * Isolated into its own component deliberately: it re-renders every second, and
 * keeping that inside the header would drag the whole header - and anything
 * else sharing its render - along with it once per second.
 */

import { useEffect, useState } from 'react';

export function UtcClock() {
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    // Align the first tick to the next whole second so the display does not
    // appear to skip or stall on mount.
    const msToNextSecond = 1000 - (Date.now() % 1000);
    let interval: ReturnType<typeof setInterval>;
    const timeout = setTimeout(() => {
      setNow(new Date());
      interval = setInterval(() => setNow(new Date()), 1000);
    }, msToNextSecond);

    return () => {
      clearTimeout(timeout);
      clearInterval(interval);
    };
  }, []);

  return (
    <div className="flex shrink-0 flex-col items-end">
      <span className="label">UTC</span>
      <time
        className="font-mono text-xs tabular-nums text-content-primary"
        dateTime={now.toISOString()}
      >
        {now.toISOString().slice(11, 19)}
      </time>
    </div>
  );
}
