import { describe, test, expect, beforeEach, mock } from "bun:test";
import {
  refreshSharedSession,
  readStoredToken,
  writeStoredToken,
  readStoredRefreshToken,
  writeStoredRefreshToken,
  type RotatedSession,
} from "./session";

/** Installs a rotated pair the way client.ts does. */
function install(token: string | null, refreshToken: string) {
  if (token) writeStoredToken(token);
  writeStoredRefreshToken(refreshToken);
}

beforeEach(() => {
  writeStoredToken("a.old.token");
  writeStoredRefreshToken("refresh-0");
});

describe("refreshSharedSession", () => {
  test("installs the rotated pair when the exchange succeeds", async () => {
    const exchange = mock(
      (): Promise<RotatedSession> =>
        Promise.resolve({ token: "a.fresh.token", refreshToken: "refresh-1" }),
    );

    expect(await refreshSharedSession(exchange, install)).toBe(true);
    expect(readStoredToken()).toBe("a.fresh.token");
    expect(readStoredRefreshToken()).toBe("refresh-1");
    expect(exchange).toHaveBeenCalledTimes(1);
  });

  test("adopts the winner's pair when another tab spent the shared token first", async () => {
    // The winning tab stores what it got back before this one is refused.
    const exchange = mock((): Promise<RotatedSession> => {
      writeStoredToken("a.new.token");
      writeStoredRefreshToken("refresh-1");
      return Promise.resolve(null);
    });

    expect(await refreshSharedSession(exchange, install)).toBe(true);
    expect(readStoredToken()).toBe("a.new.token");
    expect(readStoredRefreshToken()).toBe("refresh-1");
    // The adopted pair is usable as-is, so no second exchange is spent on it.
    expect(exchange).toHaveBeenCalledTimes(1);
  });

  test("exchanges the winner's refresh token when it left no access token", async () => {
    const seen: string[] = [];
    const exchange = mock((rt: string): Promise<RotatedSession> => {
      seen.push(rt);
      if (seen.length === 1) {
        writeStoredToken(null);
        writeStoredRefreshToken("refresh-1");
        return Promise.resolve(null);
      }
      return Promise.resolve({ token: "a.newer.token", refreshToken: "refresh-2" });
    });

    expect(await refreshSharedSession(exchange, install)).toBe(true);
    expect(seen).toEqual(["refresh-0", "refresh-1"]);
    expect(readStoredToken()).toBe("a.newer.token");
    expect(readStoredRefreshToken()).toBe("refresh-2");
  });

  test("reports the session over when nothing rotated underneath it", async () => {
    const exchange = mock((): Promise<RotatedSession> => Promise.resolve(null));

    expect(await refreshSharedSession(exchange, install)).toBe(false);
    // Nothing moved, so there is nothing to retry with.
    expect(exchange).toHaveBeenCalledTimes(1);
    expect(readStoredRefreshToken()).toBe("refresh-0");
  });

  test("reports the session over with no refresh token stored", async () => {
    writeStoredRefreshToken(null);
    const exchange = mock((): Promise<RotatedSession> => Promise.resolve(null));

    expect(await refreshSharedSession(exchange, install)).toBe(false);
    expect(exchange).not.toHaveBeenCalled();
  });

  test("gives up when the adopted token is refused too", async () => {
    let calls = 0;
    const exchange = mock((): Promise<RotatedSession> => {
      calls++;
      if (calls === 1) {
        writeStoredToken(null);
        writeStoredRefreshToken("refresh-1");
      }
      return Promise.resolve(null);
    });

    expect(await refreshSharedSession(exchange, install)).toBe(false);
    expect(calls).toBe(2);
  });
});
