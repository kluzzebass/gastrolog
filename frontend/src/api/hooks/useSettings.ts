import { useQuery, useMutation } from "@tanstack/react-query";
import { configClient } from "../client";
import { useConfigMutation } from "./useConfig";

export function useSettings() {
  return useQuery({
    queryKey: ["settings"],
    queryFn: async () => {
      const response = await configClient.getSettings({});
      return response;
    },
  });
}

export const MAXMIND_KEEP = "__KEEP_EXISTING__";

type PutSettingsArgs = {
  auth?: {
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
      responsePaths?: string[];
      parameters?: { name: string; description: string }[];
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
  };
  maxmind?: {
    autoDownload?: boolean;
    accountId?: string;
    licenseKey?: string;
  };
  cluster?: {
    broadcastInterval?: string;
  };
  setupWizardDismissed?: boolean;
};

/** Build the auth sub-request. */
function buildAuthReq(auth: NonNullable<PutSettingsArgs["auth"]>): Record<string, unknown> {
  const req: Record<string, unknown> = {};
  if (auth.tokenDuration !== undefined) req.tokenDuration = auth.tokenDuration;
  if (auth.refreshTokenDuration !== undefined) req.refreshTokenDuration = auth.refreshTokenDuration;
  if (auth.passwordPolicy) req.passwordPolicy = auth.passwordPolicy;
  return req;
}

/** Build the maxmind sub-request, filtering out the license sentinel value. */
function buildMaxMindReq(mm: NonNullable<PutSettingsArgs["maxmind"]>): Record<string, unknown> {
  const req: Record<string, unknown> = {};
  if (mm.autoDownload !== undefined) req.autoDownload = mm.autoDownload;
  if (mm.accountId !== undefined) req.accountId = mm.accountId;
  if (mm.licenseKey !== undefined && mm.licenseKey !== MAXMIND_KEEP) req.licenseKey = mm.licenseKey;
  return req;
}

export function usePutSettings() {
  return useConfigMutation(async (args: PutSettingsArgs) => {
    const req: Record<string, unknown> = {};
    if (args.auth) req.auth = buildAuthReq(args.auth);
    if (args.query) req.query = args.query;
    if (args.scheduler) req.scheduler = args.scheduler;
    if (args.tls) req.tls = args.tls;
    if (args.lookup) req.lookup = args.lookup;
    if (args.maxmind) req.maxmind = buildMaxMindReq(args.maxmind);
    if (args.cluster) req.cluster = args.cluster;
    if (args.setupWizardDismissed !== undefined)
      req.setupWizardDismissed = args.setupWizardDismissed;
    return configClient.putSettings(req as Parameters<typeof configClient.putSettings>[0]);
  }, [["settings"], ["certificates"]]);
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
      const response = await configClient.testHTTPLookup({
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
      const response = await configClient.previewCSVLookup({
        fileId: args.fileId,
        keyColumn: args.keyColumn ?? "",
        valueColumns: args.valueColumns ?? [],
        maxRows: args.maxRows ?? 10,
      });
      return response;
    },
  });
}

export function useRegenerateJwtSecret() {
  return useConfigMutation(async () => {
    return configClient.regenerateJwtSecret({});
  }, [["settings"]]);
}
