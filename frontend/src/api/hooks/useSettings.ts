import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { systemClient } from "../client";
import { useSystemMutation } from "./useSystem";
import { decode } from "../glid";

export function useSettings() {
  return useQuery({
    queryKey: ["settings"],
    queryFn: async () => {
      const response = await systemClient.getSettings({});
      return response;
    },
  });
}

export const MAXMIND_KEEP = "__KEEP_EXISTING__";

type ServiceAuth = {
  tokenDuration?: string;
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

type ServiceQuery = {
  timeout?: string;
  maxFollowDuration?: string;
  maxResultCount?: number;
};

type ServiceScheduler = {
  maxConcurrentJobs?: number;
};

type ServiceTLS = {
  defaultCert?: string;
  enabled?: boolean;
  httpToHttpsRedirect?: boolean;
  httpsPort?: string;
};

type ServiceCluster = {
  broadcastInterval?: string;
};

export type PutServiceSettingsArgs = {
  auth?: ServiceAuth;
  query?: ServiceQuery;
  scheduler?: ServiceScheduler;
  tls?: ServiceTLS;
  cluster?: ServiceCluster;
};

export type PutLookupWire = {
  httpLookups?: {
    name: string;
    urlTemplate: string;
    headers?: Record<string, string>;
    responsePaths?: string[];
    timeout?: string;
    cacheTtl?: string;
    cacheSize?: number;
  }[];
  jsonFileLookups?: {
    name: string;
    fileId: string;
    query?: string;
    keyColumn?: string;
    valueColumns?: string[];
  }[];
  mmdbLookups?: {
    name: string;
    dbType: string;
    fileId?: string;
  }[];
  csvLookups?: {
    name: string;
    fileId: string;
    keyColumn?: string;
    valueColumns?: string[];
  }[];
  staticLookups?: {
    name: string;
    keyColumn: string;
    valueColumns: string[];
    rows: { values: Record<string, string> }[];
  }[];
  yamlFileLookups?: {
    name: string;
    fileId: string;
    query?: string;
    keyColumn?: string;
    valueColumns?: string[];
  }[];
};

export type PutMaxMindArgs = {
  maxmind: {
    autoDownload?: boolean;
    accountId?: string;
    licenseKey?: string;
  };
};

/** Build the auth sub-request. */
function buildAuthReq(auth: ServiceAuth): Record<string, unknown> {
  const req: Record<string, unknown> = {};
  if (auth.tokenDuration !== undefined) req.tokenDuration = auth.tokenDuration;
  if (auth.refreshTokenDuration !== undefined) req.refreshTokenDuration = auth.refreshTokenDuration;
  if (auth.passwordPolicy) req.passwordPolicy = auth.passwordPolicy;
  return req;
}

/** Build the maxmind sub-request, filtering out the license sentinel value. */
function buildMaxMindReq(mm: PutMaxMindArgs["maxmind"]): Record<string, unknown> {
  const req: Record<string, unknown> = {};
  if (mm.autoDownload !== undefined) req.autoDownload = mm.autoDownload;
  if (mm.accountId !== undefined) req.accountId = mm.accountId;
  if (mm.licenseKey !== undefined && mm.licenseKey !== MAXMIND_KEEP) req.licenseKey = mm.licenseKey;
  return req;
}

function encodeLookupForWire(lookup: PutLookupWire): Record<string, unknown> {
  const out: Record<string, unknown> = { ...lookup };
  if (lookup.csvLookups) {
    out.csvLookups = lookup.csvLookups.map((l) => ({
      ...l,
      fileId: l.fileId ? decode(l.fileId) : undefined,
    }));
  }
  if (lookup.jsonFileLookups) {
    out.jsonFileLookups = lookup.jsonFileLookups.map((l) => ({
      ...l,
      fileId: l.fileId ? decode(l.fileId) : undefined,
    }));
  }
  if (lookup.mmdbLookups) {
    out.mmdbLookups = lookup.mmdbLookups.map((l) => ({
      ...l,
      fileId: l.fileId ? decode(l.fileId) : undefined,
    }));
  }
  if (lookup.yamlFileLookups) {
    out.yamlFileLookups = lookup.yamlFileLookups.map((l) => ({
      ...l,
      fileId: l.fileId ? decode(l.fileId) : undefined,
    }));
  }
  return out;
}

function invalidateSettings(qc: ReturnType<typeof useQueryClient>) {
  qc.invalidateQueries({ queryKey: ["settings"] });
  qc.invalidateQueries({ queryKey: ["system"] });
}

export function usePutServiceSettings() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (args: PutServiceSettingsArgs) => {
      const req: Record<string, unknown> = {};
      if (args.auth) req.auth = buildAuthReq(args.auth);
      if (args.query) req.query = args.query;
      if (args.scheduler) req.scheduler = args.scheduler;
      if (args.tls) req.tls = args.tls;
      if (args.cluster) req.cluster = args.cluster;
      return systemClient.putServiceSettings(req as Parameters<typeof systemClient.putServiceSettings>[0]);
    },
    onSuccess: () => {
      invalidateSettings(qc);
    },
  });
}

/** Lookup writes rely on WatchSystem (config_version) to refresh caches — no onSuccess invalidation. */
export function usePutLookupSettings() {
  return useMutation({
    mutationFn: async (lookup: PutLookupWire) => {
      const wire = encodeLookupForWire(lookup);
      return systemClient.putLookupSettings({
        lookup: wire as Parameters<typeof systemClient.putLookupSettings>[0]["lookup"],
      });
    },
  });
}

export function usePutMaxMindSettings() {
  return useMutation({
    mutationFn: async (args: PutMaxMindArgs) => {
      return systemClient.putMaxMindSettings({
        maxmind: buildMaxMindReq(args.maxmind) as Parameters<typeof systemClient.putMaxMindSettings>[0]["maxmind"],
      });
    },
  });
}

export function usePutSetupSettings() {
  return useMutation({
    mutationFn: async (setupWizardDismissed: boolean) => {
      return systemClient.putSetupSettings({ setupWizardDismissed });
    },
  });
}

type TestHTTPLookupArgs = {
  config: {
    name: string;
    urlTemplate: string;
    headers?: Record<string, string>;
    responsePaths?: string[];
    timeout?: string;
    cacheTtl?: string;
    cacheSize?: number;
  };
  values: Record<string, string>;
};

export function useTestHTTPLookup() {
  return useMutation({
    mutationFn: async (args: TestHTTPLookupArgs) => {
      const response = await systemClient.testHTTPLookup({
        config: args.config,
        values: args.values,
      });
      return response;
    },
  });
}

type PreviewCSVLookupArgs = {
  fileId: string;
  keyColumn?: string;
  valueColumns?: string[];
  maxRows?: number;
};

export function usePreviewCSVLookup() {
  return useMutation({
    mutationFn: async (args: PreviewCSVLookupArgs) => {
      const response = await systemClient.previewCSVLookup({
        fileId: decode(args.fileId),
        keyColumn: args.keyColumn ?? "",
        valueColumns: args.valueColumns ?? [],
        maxRows: args.maxRows ?? 10,
      });
      return response;
    },
  });
}

type PreviewJSONLookupArgs = {
  fileId: string;
  maxBytes?: number;
  query?: string;
  parameters?: Record<string, string>;
};

export function usePreviewJSONLookup() {
  return useMutation({
    mutationFn: async (args: PreviewJSONLookupArgs) => {
      const response = await systemClient.previewJSONLookup({
        fileId: decode(args.fileId),
        maxBytes: args.maxBytes ?? 4096,
        query: args.query ?? "",
        parameters: args.parameters ?? {},
      });
      return response;
    },
  });
}

export function usePreviewYAMLLookup() {
  return useMutation({
    mutationFn: async (args: PreviewJSONLookupArgs) => {
      const response = await systemClient.previewYAMLLookup({
        fileId: decode(args.fileId),
        maxBytes: args.maxBytes ?? 4096,
        query: args.query ?? "",
        parameters: args.parameters ?? {},
      });
      return response;
    },
  });
}

export function useDeleteLookup() {
  return useMutation({
    mutationFn: async (name: string) => {
      return systemClient.deleteLookup({ name });
    },
  });
}

export function useRegenerateJwtSecret() {
  return useSystemMutation(async () => {
    return systemClient.regenerateJwtSecret({});
  }, [["settings"]]);
}
