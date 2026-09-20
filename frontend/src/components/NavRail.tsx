/**
 * Primary navigation.
 *
 * Three presentations from one definition, because the destinations must not
 * drift apart between breakpoints:
 *   desktop  - expanded rail with labels
 *   tablet   - icon rail, labels on hover/focus via title + sr-only text
 *   mobile   - bottom bar
 */

import { NavLink } from 'react-router-dom';
import clsx from 'clsx';
import { Logo } from './Logo';
import { ConnectionBadge } from './StatusIndicator';
import type { ConnectionState } from '@/lib/types';

interface NavItem {
  to: string;
  label: string;
  icon: JSX.Element;
  /** Matches the index route only; other routes match by prefix. */
  end?: boolean;
}

/* Icons are inline 16px strokes rather than an icon font: five glyphs do not
 * justify a dependency, and inlining keeps them themeable with currentColor. */
const icon = (d: string) => (
  <svg
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.5"
    strokeLinecap="round"
    strokeLinejoin="round"
    className="h-4 w-4 shrink-0"
    aria-hidden="true"
  >
    <path d={d} />
  </svg>
);

const NAV_ITEMS: NavItem[] = [
  { to: '/', label: 'LIVE', end: true, icon: icon('M1 8h3l2-5 3 10 2-6 1.5 3H15') },
  { to: '/sessions', label: 'SESSIONS', icon: icon('M2 3h12M2 8h12M2 13h7') },
  { to: '/replay', label: 'REPLAY', icon: icon('M5 3.5v9l7-4.5-7-4.5Z') },
  { to: '/analytics', label: 'ANALYTICS', icon: icon('M2 14V7M6.5 14V3M11 14v-4M15 14V9') },
  { to: '/system', label: 'SYSTEM', icon: icon('M2 12h3l1.5-3 2 6 1.5-3H14M2 4h12') },
];

const linkClasses = ({ isActive }: { isActive: boolean }) =>
  clsx(
    'group relative flex items-center gap-3 rounded px-2.5 py-2 text-xs font-medium uppercase tracking-label transition-colors duration-fast',
    isActive
      ? 'bg-surface-inset text-content-primary'
      : 'text-content-tertiary hover:bg-surface-hover hover:text-content-secondary',
  );

/** The 2px accent bar marking the active route. */
function ActiveMarker({ isActive }: { isActive: boolean }) {
  return (
    <span
      aria-hidden="true"
      className={clsx(
        'absolute left-0 top-1/2 h-4 w-0.5 -translate-y-1/2 rounded-full bg-accent transition-opacity duration-fast',
        isActive ? 'opacity-100' : 'opacity-0',
      )}
    />
  );
}

interface NavRailProps {
  collapsed: boolean;
  connection: ConnectionState;
  environment: string;
  onToggleCollapsed: () => void;
}

export function NavRail({
  collapsed,
  connection,
  environment,
  onToggleCollapsed,
}: NavRailProps) {
  return (
    <nav
      aria-label="Primary"
      className={clsx(
        'hidden shrink-0 flex-col border-r border-line bg-surface-raised transition-[width] duration-200 md:flex',
        collapsed ? 'w-rail-collapsed' : 'w-rail',
      )}
    >
      <div
        className={clsx(
          'flex h-header items-center border-b border-line',
          collapsed ? 'justify-center px-2' : 'px-3',
        )}
      >
        <Logo variant={collapsed ? 'mark' : 'full'} />
      </div>

      <ul className="flex flex-1 flex-col gap-0.5 p-2" role="list">
        {NAV_ITEMS.map((item) => (
          <li key={item.to}>
            <NavLink
              to={item.to}
              end={item.end}
              className={linkClasses}
              title={collapsed ? item.label : undefined}
            >
              {({ isActive }) => (
                <>
                  <ActiveMarker isActive={isActive} />
                  {item.icon}
                  {/* Kept in the DOM when collapsed so the accessible name
                   * survives; only visually hidden. */}
                  <span className={collapsed ? 'sr-only' : undefined}>{item.label}</span>
                </>
              )}
            </NavLink>
          </li>
        ))}
      </ul>

      <div className="flex flex-col gap-2 border-t border-line p-2">
        {!collapsed && (
          <div className="flex flex-col gap-1.5 px-1.5 py-1">
            <ConnectionBadge state={connection} />
            <div className="flex items-center justify-between">
              <span className="label">ENV</span>
              <span className="font-mono text-2xs uppercase text-content-secondary">
                {environment}
              </span>
            </div>
          </div>
        )}

        <NavLink
          to="/settings"
          className={linkClasses}
          title={collapsed ? 'SETTINGS' : undefined}
        >
          {({ isActive }) => (
            <>
              <ActiveMarker isActive={isActive} />
              {icon('M8 10a2 2 0 1 0 0-4 2 2 0 0 0 0 4ZM13 8a5 5 0 0 1-.1 1l1.4 1-1.5 2.6-1.6-.6a5 5 0 0 1-1.7 1L9.2 15H6.8l-.3-1.7a5 5 0 0 1-1.7-1l-1.6.6L1.7 9l1.4-1a5 5 0 0 1 0-2l-1.4-1 1.5-2.6 1.6.6a5 5 0 0 1 1.7-1L6.8 1h2.4l.3 1.7a5 5 0 0 1 1.7 1l1.6-.6L14.3 6l-1.4 1c.1.3.1.7.1 1Z')}
              <span className={collapsed ? 'sr-only' : undefined}>SETTINGS</span>
            </>
          )}
        </NavLink>

        <button
          type="button"
          onClick={onToggleCollapsed}
          className="flex items-center gap-3 rounded px-2.5 py-2 text-content-tertiary transition-colors duration-fast hover:bg-surface-hover hover:text-content-secondary"
          aria-label={collapsed ? 'Expand navigation' : 'Collapse navigation'}
          aria-expanded={!collapsed}
        >
          <svg
            viewBox="0 0 16 16"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            className={clsx(
              'h-4 w-4 shrink-0 transition-transform duration-200',
              collapsed && 'rotate-180',
            )}
            aria-hidden="true"
          >
            <path d="M10 4 6 8l4 4" />
          </svg>
          <span
            className={clsx(
              'text-xs font-medium uppercase tracking-label',
              collapsed && 'sr-only',
            )}
          >
            Collapse
          </span>
        </button>
      </div>
    </nav>
  );
}

/** Mobile bottom bar. Same destinations, thumb-reachable. */
export function MobileNav() {
  return (
    <nav
      aria-label="Primary"
      className="fixed inset-x-0 bottom-0 z-40 flex border-t border-line bg-surface-raised/95 backdrop-blur-sm md:hidden"
      // Keep the bar clear of the home indicator on iOS.
      style={{ paddingBottom: 'env(safe-area-inset-bottom)' }}
    >
      {NAV_ITEMS.map((item) => (
        <NavLink
          key={item.to}
          to={item.to}
          end={item.end}
          className={({ isActive }) =>
            clsx(
              'flex flex-1 flex-col items-center gap-1 py-2.5 text-2xs font-medium uppercase tracking-label transition-colors duration-fast',
              isActive ? 'text-accent' : 'text-content-tertiary',
            )
          }
        >
          {item.icon}
          <span>{item.label}</span>
        </NavLink>
      ))}
    </nav>
  );
}
