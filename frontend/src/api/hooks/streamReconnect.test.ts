import { describe, test, expect } from "bun:test";
import { Code, ConnectError } from "@connectrpc/connect";
import { shouldReconnectAfter } from "./streamReconnect";

describe("shouldReconnectAfter — a refusal is not a disconnect", () => {
  test("a stream the caller's role cannot open is not retried", () => {
    const denied = new ConnectError("admin only", Code.PermissionDenied);
    expect(shouldReconnectAfter(denied)).toBe(false);
  });

  test("an expired token is retried, because refreshing fixes it", () => {
    const expired = new ConnectError("token expired", Code.Unauthenticated);
    expect(shouldReconnectAfter(expired)).toBe(true);
  });

  test("ordinary disconnects are retried", () => {
    for (const code of [
      Code.Unavailable,
      Code.DeadlineExceeded,
      Code.Internal,
      Code.Canceled,
    ]) {
      expect(shouldReconnectAfter(new ConnectError("dropped", code))).toBe(true);
    }
  });

  test("a non-Connect failure is retried rather than swallowed", () => {
    expect(shouldReconnectAfter(new Error("socket closed"))).toBe(true);
    expect(shouldReconnectAfter(undefined)).toBe(true);
    expect(shouldReconnectAfter("stringly typed")).toBe(true);
  });
});
