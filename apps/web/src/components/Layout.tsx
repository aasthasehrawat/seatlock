import { Link, NavLink, Outlet } from 'react-router-dom';

export function LockMark({ size = 28 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden="true">
      <rect width="32" height="32" rx="8" fill="currentColor" />
      <path
        d="M10 15v-3a6 6 0 0 1 12 0v3"
        fill="none"
        stroke="#fff"
        strokeWidth="2.5"
        strokeLinecap="round"
      />
      <rect x="8" y="15" width="16" height="11" rx="3" fill="#fff" />
    </svg>
  );
}

export default function Layout() {
  return (
    <div className="shell">
      <header className="site-header">
        <Link to="/" className="wordmark" aria-label="SeatLock home">
          <LockMark size={24} />
          <span>SeatLock</span>
        </Link>
        <nav>
          <NavLink to="/" end className="nav-link">
            Events
          </NavLink>
          <NavLink to="/admin" end className="nav-link">
            Dashboard
          </NavLink>
          <NavLink to="/admin/events/new" className="nav-link">
            New event
          </NavLink>
        </nav>
      </header>

      <main className="main">
        <Outlet />
      </main>

      <footer className="site-footer">
        <span>SeatLock. Seats held the instant you click.</span>
        <span className="muted">Demo environment. No real payments are taken.</span>
      </footer>
    </div>
  );
}
