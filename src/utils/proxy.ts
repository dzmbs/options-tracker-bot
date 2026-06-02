import { EnvHttpProxyAgent, setGlobalDispatcher } from 'undici';

/**
 * Routes all `fetch` calls through the proxy in HTTPS_PROXY / HTTP_PROXY
 * (NO_PROXY respected) when set. Node's fetch ignores these env vars by
 * default. Useful for local dev in regions where venue REST APIs geo-block
 * (e.g. Bybit/Binance REST 403 from the US) while WS streams still work —
 * WebSocket connections are NOT proxied, only REST.
 *
 * Returns true when a proxy was configured.
 */
export function configureProxyFromEnv(): boolean {
  const configured =
    process.env['HTTPS_PROXY'] ??
    process.env['https_proxy'] ??
    process.env['HTTP_PROXY'] ??
    process.env['http_proxy'];
  if (!configured) return false;
  setGlobalDispatcher(new EnvHttpProxyAgent());
  return true;
}
