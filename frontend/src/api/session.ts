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

/** How long to defer to another tab's refresh before going ahead regardless. */
const LOCK_WAIT_MS = 10_000;

/**
 * Runs fn with no other tab of this origin refreshing at the same time.
 *
 * Web Locks is `[SecureContext]`-gated, so it is absent over plain HTTP —
 * which includes a NodePort deployment reached by IP, the very setup where
 * tabs are most likely to land on different nodes. There, and in test DOMs,
 * the exchange runs unserialized: two tabs can still collide, leaving a
 * window of about one round-trip in which the loser sees a refused exchange
 * before the winner's result reaches storage. The recovery below handles the
 * rest of that window; it does not close this part of it.
 *
 * The wait is bounded. A tab whose refresh never settles keeps the lock —
 * aborting the wait does not revoke a granted lock — and every other tab's
 * refresh is awaited inside the response interceptor, so waiting forever
 * would hang them with no error to show. Losing the race is recoverable;
 * hanging is not.
 */
async function withRefreshLock(fn: () => Promise<boolean>): Promise<boolean> {
  const { locks } = globalThis.navigator as { locks?: LockManager };
  if (!locks) return fn();

  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), LOCK_WAIT_MS);
  try {
    // An outcome means fn ran to completion under the lock. Null means the
    // wait was abandoned before it was granted — so go ahead unserialized.
    const outcome = await locks
      .request(REFRESH_LOCK, { signal: abort.signal }, async () => ({ live: await fn() }))
      .catch(() => null);
    return outcome ? outcome.live : await fn();
  } finally {
    clearTimeout(timer);
  }
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
