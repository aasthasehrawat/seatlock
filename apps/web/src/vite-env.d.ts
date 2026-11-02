/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_INVENTORY_API_URL?: string;
  readonly VITE_ORDER_API_URL?: string;
  readonly VITE_PAYMENT_API_URL?: string;
  readonly VITE_NOTIFICATION_API_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
