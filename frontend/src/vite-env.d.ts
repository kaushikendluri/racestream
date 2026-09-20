/// <reference types="vite/client" />

/**
 * Typed build-time environment.
 *
 * Declaring these explicitly means a typo in a variable name is a compile
 * error rather than a silent `undefined` at runtime. Only VITE_-prefixed
 * variables reach the browser bundle.
 */
interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_WS_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
