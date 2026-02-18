import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";

export function useConfig() {
  return useQuery({
    queryKey: ["config"],
    queryFn: async () => {
      const response = await configClient.getConfig({});
      return response;
    },
    staleTime: 0,
    refetchInterval: 10_000,
  });
}

export function usePutFilter() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { id: string; name: string; expression: string }) => {
      await configClient.putFilter({
        config: { id: args.id, name: args.name, expression: args.expression },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useDeleteFilter() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteFilter({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function usePutRotationPolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      name: string;
      maxBytes: bigint;
      maxRecords: bigint;
      maxAgeSeconds: bigint;
      cron: string;
    }) => {
      await configClient.putRotationPolicy({
        config: {
          id: args.id,
          name: args.name,
          maxBytes: args.maxBytes,
          maxRecords: args.maxRecords,
          maxAgeSeconds: args.maxAgeSeconds,
          cron: args.cron,
        },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useDeleteRotationPolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteRotationPolicy({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function usePutRetentionPolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      name: string;
      maxAgeSeconds: bigint;
      maxBytes: bigint;
      maxChunks: bigint;
    }) => {
      await configClient.putRetentionPolicy({
        config: {
          id: args.id,
          name: args.name,
          maxAgeSeconds: args.maxAgeSeconds,
          maxBytes: args.maxBytes,
          maxChunks: args.maxChunks,
        },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useDeleteRetentionPolicy() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteRetentionPolicy({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useServerConfig() {
  return useQuery({
    queryKey: ["serverConfig"],
    queryFn: async () => {
      const response = await configClient.getServerConfig({});
      return response;
    },
  });
}

export const JWT_KEEP = "__KEEP_EXISTING__";

export function usePutServerConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      tokenDuration?: string;
      jwtSecret?: string;
      minPasswordLength?: number;
      maxConcurrentJobs?: number;
      tlsDefaultCert?: string;
      tlsEnabled?: boolean;
      httpToHttpsRedirect?: boolean;
      requireMixedCase?: boolean;
      requireDigit?: boolean;
      requireSpecial?: boolean;
      maxConsecutiveRepeats?: number;
      forbidAnimalNoise?: boolean;
    }) => {
      const req: Record<string, unknown> = {};
      if (args.tokenDuration !== undefined) req.tokenDuration = args.tokenDuration;
      if (args.jwtSecret !== undefined && args.jwtSecret !== JWT_KEEP)
        req.jwtSecret = args.jwtSecret;
      if (args.minPasswordLength !== undefined)
        req.minPasswordLength = args.minPasswordLength;
      if (args.maxConcurrentJobs !== undefined)
        req.maxConcurrentJobs = args.maxConcurrentJobs;
      if (args.tlsDefaultCert !== undefined)
        req.tlsDefaultCert = args.tlsDefaultCert;
      if (args.tlsEnabled !== undefined) req.tlsEnabled = args.tlsEnabled;
      if (args.httpToHttpsRedirect !== undefined)
        req.httpToHttpsRedirect = args.httpToHttpsRedirect;
      if (args.requireMixedCase !== undefined)
        req.requireMixedCase = args.requireMixedCase;
      if (args.requireDigit !== undefined)
        req.requireDigit = args.requireDigit;
      if (args.requireSpecial !== undefined)
        req.requireSpecial = args.requireSpecial;
      if (args.maxConsecutiveRepeats !== undefined)
        req.maxConsecutiveRepeats = args.maxConsecutiveRepeats;
      if (args.forbidAnimalNoise !== undefined)
        req.forbidAnimalNoise = args.forbidAnimalNoise;
      await configClient.putServerConfig(req as Parameters<typeof configClient.putServerConfig>[0]);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["serverConfig"] });
      qc.invalidateQueries({ queryKey: ["certificates"] });
    },
  });
}

export function useCertificates() {
  return useQuery({
    queryKey: ["certificates"],
    queryFn: async () => {
      const response = await configClient.listCertificates({});
      return response;
    },
  });
}

export function useCertificate(id: string | null) {
  return useQuery({
    queryKey: ["certificate", id],
    queryFn: async () => {
      if (!id) return null;
      const response = await configClient.getCertificate({ id });
      return response;
    },
    enabled: !!id,
  });
}

export function usePutCertificate() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      name: string;
      certPem?: string;
      keyPem?: string;
      certFile?: string;
      keyFile?: string;
      setAsDefault?: boolean;
    }) => {
      await configClient.putCertificate({
        id: args.id,
        name: args.name,
        certPem: args.certPem ?? "",
        keyPem: args.keyPem ?? "",
        certFile: args.certFile ?? "",
        keyFile: args.keyFile ?? "",
        setAsDefault: args.setAsDefault ?? false,
      });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["certificates"] });
      qc.invalidateQueries({ queryKey: ["serverConfig"] });
    },
  });
}

export function useDeleteCertificate() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteCertificate({ id });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["certificates"] });
      qc.invalidateQueries({ queryKey: ["serverConfig"] });
    },
  });
}
