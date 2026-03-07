/* eslint-disable sonarjs/no-hardcoded-passwords */
import { describe, test, expect, beforeEach } from "bun:test";
import { renderHook, waitFor, act } from "@testing-library/react";
import { installMockClients, m } from "../../../test/api-mock";
import { createTestQueryClient, wrapper } from "../../../test/render";

const mocks = installMockClients();

import {
  useAuthStatus,
  useLogin,
  useListUsers,
  useCreateUser,
  useDeleteUser,
} from "./useAuth";

beforeEach(() => {
  m(mocks.authClient, "getAuthStatus").mockClear();
  m(mocks.authClient, "login").mockClear();
  m(mocks.authClient, "listUsers").mockClear();
  m(mocks.authClient, "createUser").mockClear();
  m(mocks.authClient, "deleteUser").mockClear();
});

describe("useAuthStatus", () => {
  test("fetches auth status", async () => {
    m(mocks.authClient, "getAuthStatus").mockResolvedValueOnce({
      needsSetup: false,
    });

    const { result } = renderHook(() => useAuthStatus(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.needsSetup).toBe(false);
  });

  test("does not retry on failure", async () => {
    m(mocks.authClient, "getAuthStatus").mockRejectedValueOnce(new Error("unauthorized"));

    const { result } = renderHook(() => useAuthStatus(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isError).toBe(true));
    // retry: false means exactly 1 call.
    expect(m(mocks.authClient, "getAuthStatus")).toHaveBeenCalledTimes(1);
  });
});

describe("useLogin", () => {
  test("calls login and invalidates auth queries on success", async () => {
    m(mocks.authClient, "login").mockResolvedValueOnce({
      token: { token: "jwt.payload.sig" },
      refreshToken: "refresh-tok",
    });
    const qc = createTestQueryClient();
    qc.setQueryData(["authStatus"], { authenticated: false });

    const { result } = renderHook(() => useLogin(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({ username: "admin", password: "secret" });
    });

    expect(m(mocks.authClient, "login")).toHaveBeenCalledWith({
      username: "admin",
      password: "secret",
    });
    expect(qc.getQueryState(["authStatus"])?.isInvalidated).toBe(true);
  });
});

describe("useListUsers", () => {
  test("fetches user list", async () => {
    const users = [
      { id: "u1", username: "alice", role: "admin" },
      { id: "u2", username: "bob", role: "viewer" },
    ];
    m(mocks.authClient, "listUsers").mockResolvedValueOnce({ users });

    const { result } = renderHook(() => useListUsers(), { wrapper: wrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toHaveLength(2);
  });
});

describe("useCreateUser", () => {
  test("creates user and invalidates user list", async () => {
    m(mocks.authClient, "createUser").mockResolvedValueOnce({});
    const qc = createTestQueryClient();
    qc.setQueryData(["users"], []);

    const { result } = renderHook(() => useCreateUser(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync({ username: "carol", password: "pw", role: "viewer" });
    });

    expect(m(mocks.authClient, "createUser")).toHaveBeenCalledWith({
      username: "carol",
      password: "pw",
      role: "viewer",
    });
    expect(qc.getQueryState(["users"])?.isInvalidated).toBe(true);
  });
});

describe("useDeleteUser", () => {
  test("deletes user by id", async () => {
    m(mocks.authClient, "deleteUser").mockResolvedValueOnce({});
    const qc = createTestQueryClient();

    const { result } = renderHook(() => useDeleteUser(), { wrapper: wrapper(qc) });

    await act(async () => {
      await result.current.mutateAsync("u1");
    });

    expect(m(mocks.authClient, "deleteUser")).toHaveBeenCalledWith({ id: "u1" });
  });
});
