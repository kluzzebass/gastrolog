import { useReducer } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { EyeIcon, EyeOffIcon } from "../icons";
import { useSettings, usePutSettings, JWT_KEEP } from "../../api/hooks/useSettings";
import { useCertificates } from "../../api/hooks/useCertificates";
import { useToast } from "../Toast";
import { FormField, TextInput, NumberInput } from "./FormField";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { ExpandableCard } from "./ExpandableCard";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { extractMessage } from "../../utils/errors";

// ── Form reducer ─────────────────────────────────────────────────────

interface ServiceFormState {
  tokenDuration: string;
  jwtSecret: string;
  minPwLen: string;
  maxJobs: string;
  tlsDefaultCert: string;
  tlsEnabled: boolean;
  httpToHttpsRedirect: boolean;
  httpsPort: string;
  requireMixedCase: boolean;
  requireDigit: boolean;
  requireSpecial: boolean;
  maxConsecutiveRepeats: string;
  forbidAnimalNoise: boolean;
  refreshTokenDuration: string;
  maxFollowDuration: string;
  queryTimeout: string;
  maxResultCount: string;
  broadcastInterval: string;
  initialized: boolean;
  showSecret: boolean;
}

type ServiceFormAction =
  | { type: "init"; data: any }
  | { type: "reset"; data: any }
  | { type: "set"; field: keyof ServiceFormState; value: string | boolean }
  | { type: "toggleSecret" };

function fieldsFromData(data: any): Omit<ServiceFormState, "showSecret"> {
  const auth = data.auth;
  const pp = auth?.passwordPolicy;
  const query = data.query;
  const sched = data.scheduler;
  const tls = data.tls;
  return {
    tokenDuration: auth?.tokenDuration ?? "",
    jwtSecret: auth?.jwtSecretConfigured ? JWT_KEEP : "",
    minPwLen: pp?.minLength ? String(pp.minLength) : "8",
    maxJobs: sched?.maxConcurrentJobs ? String(sched.maxConcurrentJobs) : "4",
    tlsDefaultCert: tls?.defaultCert ?? "",
    tlsEnabled: tls?.enabled ?? false,
    httpToHttpsRedirect: tls?.httpToHttpsRedirect ?? false,
    httpsPort: tls?.httpsPort ?? "",
    requireMixedCase: pp?.requireMixedCase ?? false,
    requireDigit: pp?.requireDigit ?? false,
    requireSpecial: pp?.requireSpecial ?? false,
    maxConsecutiveRepeats: pp?.maxConsecutiveRepeats ? String(pp.maxConsecutiveRepeats) : "0",
    forbidAnimalNoise: pp?.forbidAnimalNoise ?? false,
    refreshTokenDuration: auth?.refreshTokenDuration ?? "",
    maxFollowDuration: query?.maxFollowDuration ?? "",
    queryTimeout: query?.timeout ?? "",
    maxResultCount: query?.maxResultCount ? String(query.maxResultCount) : "10000",
    broadcastInterval: data.cluster?.broadcastInterval || "5s",
    initialized: true,
  };
}

const INITIAL_STATE: ServiceFormState = {
  tokenDuration: "", jwtSecret: "", minPwLen: "", maxJobs: "",
  tlsDefaultCert: "", tlsEnabled: false, httpToHttpsRedirect: false,
  httpsPort: "", requireMixedCase: false, requireDigit: false,
  requireSpecial: false, maxConsecutiveRepeats: "", forbidAnimalNoise: false,
  refreshTokenDuration: "", maxFollowDuration: "", queryTimeout: "",
  maxResultCount: "", broadcastInterval: "", initialized: false,
  showSecret: false,
};

function serviceReducer(state: ServiceFormState, action: ServiceFormAction): ServiceFormState {
  switch (action.type) {
    case "init":
      if (state.initialized) return state;
      return { ...fieldsFromData(action.data), showSecret: false };
    case "reset":
      return { ...fieldsFromData(action.data), showSecret: state.showSecret };
    case "set":
      return { ...state, [action.field]: action.value };
    case "toggleSecret":
      return { ...state, showSecret: !state.showSecret };
  }
}

/** Parse a Go duration string (e.g. "1h30m", "15m", "90s") into total seconds, or null if unparseable. */
function parseDurationSeconds(s: string): number | null {
  if (!s.trim()) return null;
  let total = 0;
  let rest = s.trim();
  let matched = false;
  while (rest.length > 0) {
    const m = /^(\d+(?:\.\d+)?)([hms])/.exec(rest);
    if (!m) return null;
    const val = parseFloat(m[1]!);
    const unit = m[2]!;
    if (unit === "h") total += val * 3600;
    else if (unit === "m") total += val * 60;
    else total += val;
    rest = rest.slice(m[0].length);
    matched = true;
  }
  return matched ? total : null;
}

// eslint-disable-next-line sonarjs/cognitive-complexity -- inherently complex settings form with many fields, cards, and dirty tracking
export function ServiceSettings({ dark, noAuth }: Readonly<{ dark: boolean; noAuth?: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useSettings();
  const { data: certData } = useCertificates();
  const putConfig = usePutSettings();
  const { addToast } = useToast();

  const [s, dispatch] = useReducer(serviceReducer, INITIAL_STATE);
  const set = (field: keyof ServiceFormState) => (value: string | boolean) =>
    dispatch({ type: "set", field, value });

  const certs = certData?.certificates ?? [];
  const certIdSet = new Set(certs.map((c) => c.id));

  if (data && !s.initialized) {
    dispatch({ type: "init", data });
  }

  const dirty =
    s.initialized &&
    data &&
    (s.tokenDuration !== (data.auth?.tokenDuration ?? "") ||
      (s.jwtSecret !== JWT_KEEP && s.jwtSecret !== "") ||
      s.minPwLen !== String(data.auth?.passwordPolicy?.minLength || 8) ||
      s.maxJobs !== String(data.scheduler?.maxConcurrentJobs || 4) ||
      s.tlsDefaultCert !== (data.tls?.defaultCert ?? "") ||
      s.tlsEnabled !== (data.tls?.enabled ?? false) ||
      s.httpToHttpsRedirect !== (data.tls?.httpToHttpsRedirect ?? false) ||
      s.httpsPort !== (data.tls?.httpsPort ?? "") ||
      s.requireMixedCase !== (data.auth?.passwordPolicy?.requireMixedCase ?? false) ||
      s.requireDigit !== (data.auth?.passwordPolicy?.requireDigit ?? false) ||
      s.requireSpecial !== (data.auth?.passwordPolicy?.requireSpecial ?? false) ||
      s.maxConsecutiveRepeats !== String(data.auth?.passwordPolicy?.maxConsecutiveRepeats || 0) ||
      s.forbidAnimalNoise !== (data.auth?.passwordPolicy?.forbidAnimalNoise ?? false) ||
      s.refreshTokenDuration !== (data.auth?.refreshTokenDuration ?? "") ||
      s.maxFollowDuration !== (data.query?.maxFollowDuration ?? "") ||
      s.queryTimeout !== (data.query?.timeout ?? "") ||
      s.maxResultCount !== String(data.query?.maxResultCount || 10000) ||
      s.broadcastInterval !== (data.cluster?.broadcastInterval || "5s"));

  const handleSave = async () => {
    const hasCert = certIdSet.has(s.tlsDefaultCert);
    const effectiveTls = hasCert ? s.tlsEnabled : false;
    const effectiveRedirect = hasCert ? s.httpToHttpsRedirect : false;
    const effectiveJwtSecret = s.jwtSecret === JWT_KEEP ? JWT_KEEP : s.jwtSecret;
    const effectiveMinPwLen = parseInt(s.minPwLen, 10) || 8;
    const effectiveMaxJobs = parseInt(s.maxJobs, 10) || 4;
    const effectiveMaxRepeats = parseInt(s.maxConsecutiveRepeats, 10) || 0;
    const effectiveMaxResultCount = parseInt(s.maxResultCount, 10) || 0;
    const effectiveBroadcast = s.broadcastInterval || undefined;
    try {
      await putConfig.mutateAsync({
        auth: {
          tokenDuration: s.tokenDuration,
          jwtSecret: effectiveJwtSecret,
          refreshTokenDuration: s.refreshTokenDuration,
          passwordPolicy: {
            minLength: effectiveMinPwLen,
            requireMixedCase: s.requireMixedCase,
            requireDigit: s.requireDigit,
            requireSpecial: s.requireSpecial,
            maxConsecutiveRepeats: effectiveMaxRepeats,
            forbidAnimalNoise: s.forbidAnimalNoise,
          },
        },
        query: {
          timeout: s.queryTimeout,
          maxFollowDuration: s.maxFollowDuration,
          maxResultCount: effectiveMaxResultCount,
        },
        scheduler: {
          maxConcurrentJobs: effectiveMaxJobs,
        },
        tls: {
          defaultCert: s.tlsDefaultCert,
          enabled: effectiveTls,
          httpToHttpsRedirect: effectiveRedirect,
          httpsPort: s.httpsPort,
        },
        cluster: {
          broadcastInterval: effectiveBroadcast,
        },
      });
      addToast("Server configuration updated", "info");
    } catch (err: unknown) {
      addToast(extractMessage(err, "Failed to update server configuration"), "error");
    }
  };

  const handleReset = () => {
    if (data) dispatch({ type: "reset", data });
  };

  const { toggle, isExpanded } = useExpandedCards({
    auth: true,
    password: false,
    scheduler: false,
    query: false,
    tls: false,
    cluster: false,
  });

  return (
    <div>
      {isLoading ? (
        <LoadingPlaceholder dark={dark} />
      ) : (
        <div className="flex flex-col gap-3">
          <ExpandableCard
            id="Authentication"
            dark={dark}
            expanded={isExpanded("auth")}
            onToggle={() => toggle("auth")}
            monoTitle={false}
            typeBadge={noAuth ? "disabled" : undefined}
          >
            <div className={`flex flex-col gap-4 ${noAuth ? "opacity-50" : ""}`} aria-disabled={noAuth || undefined}>
              <FormField
                label="Token Duration"
                description="How long access tokens remain valid. Short-lived tokens are more secure when paired with refresh tokens."
                dark={dark}
              >
                <TextInput
                  value={s.tokenDuration}
                  onChange={set("tokenDuration")}
                  placeholder="15m"
                  dark={dark}
                  mono
                  examples={["15m", "1h", "24h"]}
                />
                {(() => {
                  const secs = parseDurationSeconds(s.tokenDuration);
                  if (secs !== null && secs < 60)
                    return <p className="text-[0.75em] text-amber-500 mt-1">Must be at least 1 minute</p>;
                  return null;
                })()}
              </FormField>

              <FormField
                label="Refresh Token Duration"
                description="How long refresh tokens remain valid. Users must re-authenticate after this period of inactivity."
                dark={dark}
              >
                <TextInput
                  value={s.refreshTokenDuration}
                  onChange={set("refreshTokenDuration")}
                  placeholder="168h"
                  dark={dark}
                  mono
                  examples={["24h", "168h", "720h"]}
                />
                {(() => {
                  const secs = parseDurationSeconds(s.refreshTokenDuration);
                  const tokenSecs = parseDurationSeconds(s.tokenDuration);
                  if (secs !== null && secs < 3600)
                    return <p className="text-[0.75em] text-amber-500 mt-1">Must be at least 1 hour</p>;
                  if (secs !== null && tokenSecs !== null && secs <= tokenSecs)
                    return <p className="text-[0.75em] text-amber-500 mt-1">Must be longer than the token duration</p>;
                  return null;
                })()}
              </FormField>

              <FormField
                label="JWT Secret"
                description="The signing key used for authentication tokens. Never shown; paste a new value to change. Changing this will invalidate all existing sessions."
                dark={dark}
              >
                <div className="relative">
                  <input
                    type={s.showSecret ? "text" : "password"}
                    value={s.jwtSecret === JWT_KEEP ? "" : s.jwtSecret}
                    onChange={(e) => dispatch({ type: "set", field: "jwtSecret", value: e.target.value })}
                    placeholder={data?.auth?.jwtSecretConfigured ? "•••••••• (paste to replace)" : "Set JWT secret"}
                    className={`w-full px-2.5 py-1.5 pr-9 text-[0.85em] font-mono border rounded focus:outline-none transition-colors ${c(
                      "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                      "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
                    )}`}
                  />
                  <button
                    type="button"
                    onClick={() => dispatch({ type: "toggleSecret" })}
                    className={`absolute right-2 top-1/2 -translate-y-1/2 transition-colors ${c(
                      "text-text-ghost hover:text-text-muted",
                      "text-light-text-ghost hover:text-light-text-muted",
                    )}`}
                  >
                    {s.showSecret ? (
                      <EyeOffIcon className="w-4 h-4" />
                    ) : (
                      <EyeIcon className="w-4 h-4" />
                    )}
                  </button>
                </div>
              </FormField>
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="Password Policy"
            dark={dark}
            expanded={isExpanded("password")}
            onToggle={() => toggle("password")}
            monoTitle={false}
            typeBadge={noAuth ? "disabled" : undefined}
          >
            <div className={`flex flex-col gap-4 ${noAuth ? "opacity-50" : ""}`} aria-disabled={noAuth || undefined}>
              <div className="flex items-baseline gap-4">
                <FormField
                  label="Minimum length"
                  dark={dark}
                >
                  <NumberInput
                    value={s.minPwLen}
                    onChange={set("minPwLen")}
                    placeholder="8"
                    dark={dark}
                    min={1}
                  />
                </FormField>

                <FormField
                  label="Max consecutive repeats"
                  dark={dark}
                >
                  <NumberInput
                    value={s.maxConsecutiveRepeats}
                    onChange={set("maxConsecutiveRepeats")}
                    placeholder="0 (no limit)"
                    dark={dark}
                    min={0}
                  />
                </FormField>
              </div>

              <div className="flex flex-col gap-2.5">
                <Checkbox checked={s.requireMixedCase} onChange={set("requireMixedCase")} label="Require mixed case (upper + lowercase)" dark={dark} />
                <Checkbox checked={s.requireDigit} onChange={set("requireDigit")} label="Require digit (0-9)" dark={dark} />
                <Checkbox checked={s.requireSpecial} onChange={set("requireSpecial")} label="Require special character" dark={dark} />
                <Checkbox checked={s.forbidAnimalNoise} onChange={set("forbidAnimalNoise")} label="Forbid animal noises (moo, woof, meow, ...)" dark={dark} />
              </div>
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="Scheduler"
            dark={dark}
            expanded={isExpanded("scheduler")}
            onToggle={() => toggle("scheduler")}
            monoTitle={false}
          >
            <div className="flex flex-col gap-4">
              <FormField
                label="Max Concurrent Jobs"
                description="Maximum number of scheduler jobs (index builds, rotation, retention) that can run in parallel."
                dark={dark}
              >
                <NumberInput
                  value={s.maxJobs}
                  onChange={set("maxJobs")}
                  placeholder="4"
                  dark={dark}
                  min={1}
                  examples={["2", "4", "8"]}
                />
              </FormField>
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="Query"
            dark={dark}
            expanded={isExpanded("query")}
            onToggle={() => toggle("query")}
            monoTitle={false}
          >
            <div className="flex flex-col gap-4">
              <FormField
                label="Query Timeout"
                description="Maximum duration for Search, Histogram, and GetContext queries. Leave empty to disable."
                dark={dark}
              >
                <TextInput
                  value={s.queryTimeout}
                  onChange={set("queryTimeout")}
                  placeholder="30s"
                  dark={dark}
                  mono
                  examples={["15s", "30s", "1m", "5m"]}
                />
              </FormField>

              <FormField
                label="Max Follow Duration"
                description="Maximum lifetime for a Follow stream before the server closes it. Leave empty to disable."
                dark={dark}
              >
                <TextInput
                  value={s.maxFollowDuration}
                  onChange={set("maxFollowDuration")}
                  placeholder="4h"
                  dark={dark}
                  mono
                  examples={["1h", "4h", "8h", "24h"]}
                />
              </FormField>

              <FormField
                label="Max Result Count"
                description="Maximum number of records a single search request can return. Set to 0 for unlimited."
                dark={dark}
              >
                <NumberInput
                  value={s.maxResultCount}
                  onChange={set("maxResultCount")}
                  dark={dark}
                  min={0}
                  examples={["1000", "10000", "100000"]}
                />
              </FormField>
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="TLS"
            dark={dark}
            expanded={isExpanded("tls")}
            onToggle={() => toggle("tls")}
            monoTitle={false}
            typeBadge={s.tlsEnabled ? "enabled" : undefined}
            typeBadgeAccent={s.tlsEnabled}
          >
            <div className="flex flex-col gap-4">
              <FormField
                label="Default certificate"
                description="Certificate used for HTTPS. Set in Certificates tab."
                dark={dark}
              >
                <select
                  value={s.tlsDefaultCert}
                  onChange={(e) => dispatch({ type: "set", field: "tlsDefaultCert", value: e.target.value })}
                  className={`w-full px-2.5 py-1.5 text-[0.85em] border rounded focus:outline-none transition-colors ${c(
                    "bg-ink-surface border-ink-border text-text-bright focus:border-copper-dim",
                    "bg-light-surface border-light-border text-light-text-bright focus:border-copper",
                  )}`}
                >
                  <option value="">-- none --</option>
                  {certs
                    .toSorted((a, b) => (a.name || a.id).localeCompare(b.name || b.id))
                    .map((cert) => (
                    <option key={cert.id} value={cert.id}>
                      {cert.name || cert.id}
                    </option>
                  ))}
                </select>
              </FormField>

              {s.tlsDefaultCert && (
                <>
                  <FormField
                    label="Enable TLS (HTTPS)"
                    description="Serve HTTPS when a default certificate is set"
                    dark={dark}
                  >
                    <Checkbox
                      checked={s.tlsEnabled}
                      onChange={set("tlsEnabled")}
                      dark={dark}
                    />
                  </FormField>
                  {s.tlsEnabled && (
                    <>
                      <FormField
                        label="HTTPS port"
                        description="Port for the HTTPS listener. Leave empty for HTTP port + 1."
                        dark={dark}
                      >
                        <input
                          type="text"
                          inputMode="numeric"
                          value={s.httpsPort}
                          onChange={(e) => dispatch({ type: "set", field: "httpsPort", value: e.target.value })}
                          placeholder="auto"
                          className={`w-full px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors ${c(
                            "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                            "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
                          )}`}
                        />
                      </FormField>
                      <FormField
                        label="Redirect HTTP to HTTPS"
                        description="Redirect plain HTTP requests to HTTPS"
                        dark={dark}
                      >
                        <Checkbox
                          checked={s.httpToHttpsRedirect}
                          onChange={set("httpToHttpsRedirect")}
                          dark={dark}
                        />
                      </FormField>
                    </>
                  )}
                </>
              )}
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="Broadcasting"
            dark={dark}
            expanded={isExpanded("cluster")}
            onToggle={() => toggle("cluster")}
            monoTitle={false}
          >
            <div className="flex flex-col gap-4">
              <FormField
                label="Broadcast Interval"
                description="How often each node broadcasts its stats to peers. Lower values give fresher data but increase network traffic."
                dark={dark}
              >
                <TextInput
                  value={s.broadcastInterval}
                  onChange={set("broadcastInterval")}
                  placeholder="5s"
                  dark={dark}
                  mono
                  examples={["3s", "5s", "10s", "30s"]}
                />
                {(() => {
                  const secs = parseDurationSeconds(s.broadcastInterval);
                  if (s.broadcastInterval && secs === null)
                    return <p className="text-[0.75em] text-amber-500 mt-1">Invalid duration format</p>;
                  if (secs !== null && secs < 1)
                    return <p className="text-[0.75em] text-amber-500 mt-1">Must be at least 1 second</p>;
                  return null;
                })()}
              </FormField>
            </div>
          </ExpandableCard>

          <div className="flex gap-2 mt-2">
            <Button
              onClick={handleSave}
              disabled={!dirty || putConfig.isPending}
            >
              {putConfig.isPending ? "Saving..." : "Save"}
            </Button>
            {dirty && (
              <Button variant="ghost" onClick={handleReset} dark={dark}>
                Reset
              </Button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
