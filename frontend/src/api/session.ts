/**
 * The login session as every tab of this origin sees it.
 *
 * Both tokens live in localStorage, so tabs share one session. The server
 * consumes a refresh token exactly once — rotation is a single step there —
 * which means two tabs refreshing at the same moment cannot both win. This
 * module owns that: it serializes the exchange where the browser can, and
 * when a tab loses anyway it takes up what the winner stored instead of
 * clearing the keys the winner is using.
 */

const TOKEN_KEY = "gastrolog_token";
const REFRESH_TOKEN_KEY = "gastrolog_refresh_token";

export function readStoredToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function writeStoredToken(token: string | null): void {
  if (token) {
    localStorage.setItem(TOKEN_KEY, token);
  } else {
    localStorage.removeItem(TOKEN_KEY);
  }
}

export function readStoredRefreshToken(): string | null {
  return localStorage.getItem(REFRESH_TOKEN_KEY);
}

export function writeStoredRefreshToken(token: string | null): void {
  if (token) {
    localStorage.setItem(REFRESH_TOKEN_KEY, token);
  } else {
    localStorage.removeItem(REFRESH_TOKEN_KEY);
  }
}

/** The rotated pair a successful exchange yields; null when it was refused. */
export type RotatedSession = { token: string | null; refreshToken: string } | null;

/** The network half of a refresh. Resolves null when the token is refused. */
export type Exchange = (refreshToken: string) => Promise<RotatedSession>;

/** Installs a rotated pair — in-memory state and storage alike. */
export type Install = (token: string | null, refreshToken: string) => void;

const REFRESH_LOCK = "gastrolog_refresh";

/**
 * Runs fn with no other tab of this origin refreshing at the same time.
 * Web Locks is missing outside a secure context and in some test DOMs; there
 * the exchange runs unserialized and the losing tab recovers instead.
 */
function withRefreshLock(fn: () => Promise<boolean>): Promise<boolean> {
  const { locks } = globalThis.navigator as { locks?: LockManager };
  if (!locks) return fn();
  return locks.request(REFRESH_LOCK, fn).then(Boolean, () => false);
}

/**
 * Exchanges the stored refresh token for a fresh pair, surviving the case
 * where another tab spent it first. Reports whether the session came out of
 * this live; only a false answer means the user must log in again.
 */
export function refreshSharedSession(exchange: Exchange, install: Install): Promise<boolean> {
  return withRefreshLock(async () => {
    const presented = readStoredRefreshToken();
    if (!presented) return false;

    const rotated = await exchange(presented);
    if (rotated) {
      install(rotated.token, rotated.refreshToken);
      return true;
    }

    // Refused. If the stored token has moved on, another tab won the race and
    // its result is sitting in storage — adopt that rather than declaring the
    // session over and clearing it out from under the winner.
    const winner = readStoredRefreshToken();
    if (!winner || winner === presented) return false;

    const winnerToken = readStoredToken();
    if (winnerToken) {
      install(winnerToken, winner);
      return true;
    }

    const second = await exchange(winner);
    if (!second) return false;
    install(second.token, second.refreshToken);
    return true;
  });
}
