import { Link } from 'react-router-dom';

export function NotFoundPage() {
  return (
    <div className="flex flex-col items-center justify-center gap-3 px-6 py-24 text-center">
      <span className="telemetry-value text-hero text-content-disabled">404</span>
      <p className="text-sm text-content-secondary">No route matches this address.</p>
      <Link
        to="/"
        className="mt-2 rounded border border-line bg-surface-inset px-3 py-1.5 text-2xs font-medium uppercase tracking-label text-content-secondary transition-colors duration-fast hover:border-line-strong hover:text-content-primary"
      >
        Back to dashboard
      </Link>
    </div>
  );
}
