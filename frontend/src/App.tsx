/**
 * Route table.
 *
 * Flat and explicit: every route in section 7 of the spec is reachable from
 * Phase 1 onward, so navigation is testable before the screens behind it are
 * finished.
 */

import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import { AppShell } from '@/components/AppShell';
import { SystemHealthPage } from '@/pages/SystemHealth';
import { NotFoundPage } from '@/pages/NotFound';
import {
  AnalyticsPage,
  DashboardPage,
  DriverPage,
  ReplayPage,
  SessionsPage,
  SettingsPage,
} from '@/pages/Placeholder';

export function App() {
  return (
    <BrowserRouter
      // Opt into the v7 behaviours now so the upgrade is a version bump rather
      // than a behavioural change to debug later.
      future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
    >
      <Routes>
        <Route element={<AppShell />}>
          <Route index element={<DashboardPage />} />
          <Route path="sessions" element={<SessionsPage />} />
          {/* Singular alias kept because the spec names /session; both resolve
              to the same explorer rather than silently 404ing. */}
          <Route path="session" element={<Navigate to="/sessions" replace />} />
          <Route path="sessions/:sessionId" element={<SessionsPage />} />
          <Route path="replay" element={<ReplayPage />} />
          <Route path="driver/:driverId" element={<DriverPage />} />
          <Route path="analytics" element={<AnalyticsPage />} />
          <Route path="system" element={<SystemHealthPage />} />
          <Route path="settings" element={<SettingsPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
