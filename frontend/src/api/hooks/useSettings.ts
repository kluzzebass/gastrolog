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
  if (lookup.httpLookups) req.httpLookups = lookup.httpLookups;
  if (lookup.jsonFileLookups) req.jsonFileLookups = lookup.jsonFileLookups;
  if (lookup.mmdbLookups) req.mmdbLookups = lookup.mmdbLookups;
  return req;
}

export function usePutSettings() {
  return useConfigMutation(async (args: PutSettingsArgs) => {
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
