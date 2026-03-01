import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { configClient } from "../client";
import { GetConfigResponse } from "../gen/gastrolog/v1/config_pb";
import { protoSharing } from "./protoSharing";

export function useConfig() {
  return useQuery({
    queryKey: ["config"],
    queryFn: async () => {
      const response = await configClient.getConfig({});
      return response;
    },
    structuralSharing: protoSharing(GetConfigResponse.equals),
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

export function usePutRoute() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: {
      id: string;
      name: string;
      filterId: string;
      destinations: string[];
      distribution: string;
      enabled: boolean;
    }) => {
      await configClient.putRoute({
        config: {
          id: args.id,
          name: args.name,
          filterId: args.filterId,
          destinations: args.destinations.map((vaultId) => ({ vaultId })),
          distribution: args.distribution,
          enabled: args.enabled,
        },
      });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useDeleteRoute() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (id: string) => {
      await configClient.deleteRoute({ id });
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["config"] }),
  });
}

export function useSettings() {
  return useQuery({
    queryKey: ["settings"],
    queryFn: async () => {
      const response = await configClient.getSettings({});
      return response;
    },
  });
}

export const JWT_KEEP = "__KEEP_EXISTING__";
export const MAXMIND_KEEP = "__KEEP_EXISTING__";

type PutSettingsArgs = {
  auth?: {
    tokenDuration?: string;
    jwtSecret?: string;
    refreshTokenDuration?: string;
    passwordPolicy?: {
      minLength?: number;
      requireMixedCase?: boolean;
      requireDigit?: boolean;
      requireSpecial?: boolean;
      maxConsecutiveRepeats?: number;
      forbidAnimalNoise?: boolean;
    };
  };
  query?: {
    timeout?: string;
    maxFollowDuration?: string;
    maxResultCount?: number;
  };
  scheduler?: {
    maxConcurrentJobs?: number;
  };
  tls?: {
    defaultCert?: string;
    enabled?: boolean;
    httpToHttpsRedirect?: boolean;
    httpsPort?: string;
  };
  lookup?: {
    geoipDbPath?: string;
    asnDbPath?: string;
    maxmind?: {
      autoDownload?: boolean;
      accountId?: string;
      licenseKey?: string;
    };
  };
  cluster?: {
    broadcastInterval?: string;
  };
  setupWizardDismissed?: boolean;
};

/** Build the auth sub-request, filtering out the JWT sentinel value. */
function buildAuthReq(auth: NonNullable<PutSettingsArgs["auth"]>): Record<string, unknown> {
  const req: Record<string, unknown> = {};
  if (auth.tokenDuration !== undefined) req.tokenDuration = auth.tokenDuration;
  if (auth.refreshTokenDuration !== undefined) req.refreshTokenDuration = auth.refreshTokenDuration;
  if (auth.jwtSecret !== undefined && auth.jwtSecret !== JWT_KEEP) req.jwtSecret = auth.jwtSecret;
  if (auth.passwordPolicy) req.passwordPolicy = auth.passwordPolicy;
  return req;
}

/** Build the lookup sub-request, filtering out the MaxMind license sentinel value. */
function buildLookupReq(lookup: NonNullable<PutSettingsArgs["lookup"]>): Record<string, unknown> {
  const req: Record<string, unknown> = {};
  if (lookup.geoipDbPath !== undefined) req.geoipDbPath = lookup.geoipDbPath;
  if (lookup.asnDbPath !== undefined) req.asnDbPath = lookup.asnDbPath;
  if (lookup.maxmind) {
    const mm: Record<string, unknown> = {};
    if (lookup.maxmind.autoDownload !== undefined) mm.autoDownload = lookup.maxmind.autoDownload;
    if (lookup.maxmind.accountId !== undefined) mm.accountId = lookup.maxmind.accountId;
    if (lookup.maxmind.licenseKey !== undefined && lookup.maxmind.licenseKey !== MAXMIND_KEEP)
      mm.licenseKey = lookup.maxmind.licenseKey;
    req.maxmind = mm;
  }
  return req;
}

export function usePutSettings() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: PutSettingsArgs) => {
      const req: Record<string, unknown> = {};
      if (args.auth) req.auth = buildAuthReq(args.auth);
      if (args.query) req.query = args.query;
      if (args.scheduler) req.scheduler = args.scheduler;
      if (args.tls) req.tls = args.tls;
      if (args.lookup) req.lookup = buildLookupReq(args.lookup);
      if (args.cluster) req.cluster = args.cluster;
      if (args.setupWizardDismissed !== undefined)
        req.setupWizardDismissed = args.setupWizardDismissed;
      return configClient.putSettings(req as Parameters<typeof configClient.putSettings>[0]);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["settings"] });
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
      qc.invalidateQueries({ queryKey: ["settings"] });
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
      qc.invalidateQueries({ queryKey: ["settings"] });
    },
  });
}

export function usePutNodeConfig() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: { id: string; name: string }) => {
      await configClient.putNodeConfig({ config: { id: args.id, name: args.name } });
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["settings"] });
      qc.invalidateQueries({ queryKey: ["config"] });
    },
  });
}

export function useGenerateName() {
  return useMutation({
    mutationFn: async () => {
      const response = await configClient.generateName({});
      return response.name;
    },
  });
}
