import { Link, Route, Routes } from 'react-router-dom';
import Layout from './components/Layout';
import EventListPage from './pages/EventListPage';
import EventDetailPage from './pages/EventDetailPage';
import PaymentPage from './pages/PaymentPage';
import OrderStatusPage from './pages/OrderStatusPage';
import AdminDashboardPage from './pages/AdminDashboardPage';
import CreateEventPage from './pages/CreateEventPage';

function NotFound() {
  return (
    <div className="page narrow">
      <h1 className="page-title">Page not found</h1>
      <p className="muted">That page doesn&apos;t exist.</p>
      <Link className="text-link" to="/">
        Back to events
      </Link>
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<EventListPage />} />
        <Route path="/events/:id" element={<EventDetailPage />} />
        <Route path="/pay/:orderId" element={<PaymentPage />} />
        <Route path="/orders/:orderId" element={<OrderStatusPage />} />
        <Route path="/admin" element={<AdminDashboardPage />} />
        <Route path="/admin/events/new" element={<CreateEventPage />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
