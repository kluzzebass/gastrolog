import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { useCallback } from "react";
import { authClient, setToken } from "../client";

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

export function useLogout() {
  const qc = useQueryClient();
  const navigate = useNavigate();
  return useCallback(() => {
    setToken(null);
    qc.clear();
    navigate({ to: "/login" });
  }, [qc, navigate]);
}
