import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { useCallback, useMemo } from "react";
import { authClient, getToken, setToken } from "../client";

export function useAuthStatus() {
  return useQuery({
    queryKey: ["authStatus"],
    queryFn: async () => {
      const res = await authClient.getAuthStatus({});
      return res;
    },
    staleTime: 0,
    retry: false,
  });
}

export function useLogin() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { username: string; password: string }) => {
      return authClient.login(args);
    },
    onSuccess: (data) => {
      if (data.token) {
        setToken(data.token.token);
      }
      qc.invalidateQueries({ queryKey: ["authStatus"] });
    },
  });
}

export function useRegister() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { username: string; password: string }) => {
      return authClient.register(args);
    },
    onSuccess: (data) => {
      if (data.token) {
        setToken(data.token.token);
      }
      qc.invalidateQueries({ queryKey: ["authStatus"] });
    },
  });
}

/** Decode the JWT payload to extract username and role. */
function parseTokenPayload(
  token: string,
): { username: string; role: string } | null {
  try {
    const [, payloadB64] = token.split(".");
    if (!payloadB64) return null;
    const payload = JSON.parse(
      atob(payloadB64.replace(/-/g, "+").replace(/_/g, "/")),
    );
    return { username: payload.sub ?? "", role: payload.role ?? "" };
  } catch {
    return null;
  }
}

/** Returns the current user's username and role from the stored JWT. */
export function useCurrentUser(): { username: string; role: string } | null {
  const token = getToken();
  return useMemo(() => (token ? parseTokenPayload(token) : null), [token]);
}

export function useChangePassword() {
  return useMutation({
    mutationFn: async (args: {
      username: string;
      oldPassword: string;
      newPassword: string;
    }) => {
      return authClient.changePassword(args);
    },
  });
}

export function useLogout() {
  const qc = useQueryClient();
  const navigate = useNavigate();
  return useCallback(() => {
    setToken(null);
    qc.clear();
    navigate({ to: "/login" });
  }, [qc, navigate]);
}

export function useListUsers() {
  return useQuery({
    queryKey: ["users"],
    queryFn: async () => {
      const res = await authClient.listUsers({});
      return res.users;
    },
  });
}

export function useCreateUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      username: string;
      password: string;
      role: string;
    }) => {
      return authClient.createUser(args);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["users"] }),
  });
}

export function useResetPassword() {
  return useMutation({
    mutationFn: async (args: { id: string; newPassword: string }) => {
      return authClient.resetPassword(args);
    },
  });
}

export function useUpdateUserRole() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { id: string; role: string }) => {
      return authClient.updateUserRole(args);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["users"] }),
  });
}

export function useDeleteUser() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      return authClient.deleteUser({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["users"] }),
  });
}
