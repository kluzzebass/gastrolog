import { encode } from "../../api/glid";
import { useReducer, useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { LoadingPlaceholder } from "../LoadingPlaceholder";
import { useSettings, usePutServiceSettings, useRegenerateJwtSecret } from "../../api/hooks/useSettings";
import { useCertificates } from "../../api/hooks/useCertificates";
import { useToast } from "../Toast";
import { FormField, TextInput, NumberInput } from "./FormField";
import { Button } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { EligibilitySelect } from "./EligibilitySelect";
import { ExpandableCard } from "./ExpandableCard";
import { useExpandedCards } from "../../hooks/useExpandedCards";
import { extractMessage } from "../../utils/errors";
import type { GetSettingsResponse } from "../../api/gen/gastrolog/v1/system_pb";
import { ReadOnlyProvider, useReadOnly } from "../../hooks/useReadOnly";

// ── Form reducer ─────────────────────────────────────────────────────

interface ServiceFormState {
  tokenDuration: string;
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
  pipelineBacklogMax: string; // human size; per-vault pipeline backlog budget; empty = unbounded
  initialized: boolean;
}

type ServiceFormAction =
  | { type: "init"; data: GetSettingsResponse }
  | { type: "reset"; data: GetSettingsResponse }
  | { type: "set"; field: keyof ServiceFormState; value: string | boolean };

function fieldsFromData(data: GetSettingsResponse): ServiceFormState {
  const auth = data.auth;
  const pp = auth?.passwordPolicy;
  const query = data.query;
  const sched = data.scheduler;
  const tls = data.tls;
  return {
    tokenDuration: auth?.tokenDuration ?? "",
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
    pipelineBacklogMax:
      data.cluster && data.cluster.pipelineBacklogMax !== ""
        ? data.cluster.pipelineBacklogMax
        : "",
    initialized: true,
  };
}

const noAuthBadge = { text: "disabled", title: "disabled by --no-auth", helpRef: "user-management#no-auth-mode" };

const DEFAULT_CERT_HELP_REF = "certificates#default-certificate";

const INITIAL_STATE: ServiceFormState = {
  tokenDuration: "", minPwLen: "", maxJobs: "",
  tlsDefaultCert: "", tlsEnabled: false, httpToHttpsRedirect: false,
  httpsPort: "", requireMixedCase: false, requireDigit: false,
  requireSpecial: false, maxConsecutiveRepeats: "", forbidAnimalNoise: false,
  refreshTokenDuration: "", maxFollowDuration: "", queryTimeout: "",
  maxResultCount: "", broadcastInterval: "",
  pipelineBacklogMax: "", initialized: false,
};

function serviceReducer(state: ServiceFormState, action: ServiceFormAction): ServiceFormState {
  switch (action.type) {
    case "init":
      if (state.initialized) return state;
      return fieldsFromData(action.data);
    case "reset":
      return fieldsFromData(action.data);
    case "set":
      return { ...state, [action.field]: action.value };
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
  const putConfig = usePutServiceSettings();
  const regenerateJwt = useRegenerateJwtSecret();
  const { addToast } = useToast();
  const [confirmRegenerate, setConfirmRegenerate] = useState(false);

  const [s, dispatch] = useReducer(serviceReducer, INITIAL_STATE);
  const set = (field: keyof ServiceFormState) => (value: string | boolean) =>
    dispatch({ type: "set", field, value });

  const certs = certData?.certificates ?? [];
  const certIdSet = new Set(certs.map((c) => encode(c.id)));
  const hasCert = certIdSet.has(s.tlsDefaultCert);
  let tlsGateReason: string | undefined;
  if (!hasCert) tlsGateReason = "Requires a default certificate.";
  else if (!s.tlsEnabled) tlsGateReason = "Requires TLS enabled.";

  // The header badge reads what is saved, never the draft: a draft is a
  // proposal until Save, and Save forces TLS off when the saved certificate
  // is gone — that is said here, not discovered after the fact.
  const savedTlsEnabled = data?.tls?.enabled ?? false;
  const savedTlsCertMissing =
    savedTlsEnabled && certData !== undefined && !certIdSet.has(data?.tls?.defaultCert ?? "");

  // Under --no-auth the authentication and password-policy settings are
  // not consulted; a form that accepts edits into them would be saving
  // into a mode that ignores them.
  const readOnly = useReadOnly();
  const authReadOnly = readOnly || !!noAuth;

  if (data && !s.initialized) {
    dispatch({ type: "init", data });
  }

  const dirty =
    s.initialized &&
    data &&
    (s.tokenDuration !== (data.auth?.tokenDuration ?? "") ||
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
      s.broadcastInterval !== (data.cluster?.broadcastInterval || "5s") ||
      s.pipelineBacklogMax !==
        (data.cluster && data.cluster.pipelineBacklogMax !== ""
          ? data.cluster.pipelineBacklogMax
          : ""));

  const handleSave = async () => {
    const effectiveTls = hasCert ? s.tlsEnabled : false;
    const effectiveRedirect = hasCert ? s.httpToHttpsRedirect : false;
    const effectiveMinPwLen = parseInt(s.minPwLen, 10) || 8;
    const effectiveMaxJobs = parseInt(s.maxJobs, 10) || 4;
    const effectiveMaxRepeats = parseInt(s.maxConsecutiveRepeats, 10) || 0;
    const effectiveMaxResultCount = parseInt(s.maxResultCount, 10) || 0;
    const effectiveBroadcast = s.broadcastInterval || undefined;
    try {
      await putConfig.mutateAsync({
        auth: {
          tokenDuration: s.tokenDuration,
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
          // Always sent: clearing the field means 0 = unbounded, which must
          // reach the store (undefined would merge-skip and keep the old budget).
          pipelineBacklogMax: s.pipelineBacklogMax,
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
    pipeline: false,
  });

  return (
    <div>
      {isLoading ? (
        <LoadingPlaceholder dark={dark} />
      ) : (
        <div className="flex flex-col gap-3">
          <ExpandableCard
            id="Authentication"
            helpRef="user-management#authentication"
            dark={dark}
            expanded={isExpanded("auth")}
            onToggle={() => toggle("auth")}
            monoTitle={false}
            typeBadge={noAuth ? noAuthBadge : undefined}
          >
            <ReadOnlyProvider readOnly={authReadOnly}>
            <div className={`flex flex-col gap-4 ${noAuth ? "opacity-50" : ""}`} aria-disabled={noAuth || undefined}>
              <FormField
                label="Token Duration"
                description="How long access tokens remain valid. Short-lived tokens are more secure when paired with refresh tokens."
               
                dark={dark}
              >
                <TextInput
                  value={s.tokenDuration}
                  onChange={set("tokenDuration")}
                  placeholder="1h"
                  dark={dark}
                  mono
                  examples={["15m", "1h", "24h"]}
                />
                {(() => {
                  const secs = parseDurationSeconds(s.tokenDuration);
                  if (secs !== null && secs < 60)
                    return <p className="text-[0.75em] text-severity-warn mt-1">Must be at least 1 minute</p>;
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
                    return <p className="text-[0.75em] text-severity-warn mt-1">Must be at least 1 hour</p>;
                  if (secs !== null && tokenSecs !== null && secs <= tokenSecs)
                    return <p className="text-[0.75em] text-severity-warn mt-1">Must be longer than the token duration</p>;
                  return null;
                })()}
              </FormField>

              <FormField
                label="JWT Secret"
                description={data?.auth?.jwtSecretConfigured
                  ? "A signing key is configured. Regenerating will invalidate all active sessions cluster-wide."
                  : "No signing key configured. Authentication will not work until one is generated."
                }
                dark={dark}
              >
                {confirmRegenerate ? (
                  <div className="flex items-center gap-2">
                    <span className={`text-[0.8em] ${c("text-severity-warn", "text-severity-warn")}`}>
                      All users will be logged out. Continue?
                    </span>
                    <Button
                      variant="danger"
                      dark={dark}
                      onClick={async () => {
                        try {
                          await regenerateJwt.mutateAsync(undefined); // eslint-disable-line unicorn/no-useless-undefined -- required by useMutation void TArgs
                          addToast("JWT secret regenerated — all sessions invalidated", "info");
                        } catch (err: unknown) {
                          addToast(extractMessage(err, "Failed to regenerate JWT secret"), "error");
                        }
                        setConfirmRegenerate(false);
                      }}
                      disabled={regenerateJwt.isPending}
                    >
                      {regenerateJwt.isPending ? "Regenerating..." : "Confirm"}
                    </Button>
                    <Button
                      variant="ghost"
                      dark={dark}
                      onClick={() => setConfirmRegenerate(false)}
                    >
                      Cancel
                    </Button>
                  </div>
                ) : (
                  <Button
                    variant="ghost"
                    bordered
                    dark={dark}
                    onClick={() => setConfirmRegenerate(true)}
                  >
                    Regenerate Secret
                  </Button>
                )}
              </FormField>
            </div>
            </ReadOnlyProvider>
          </ExpandableCard>

          <ExpandableCard
            id="Password Policy"
            helpRef="user-management#password-policy"
            dark={dark}
            expanded={isExpanded("password")}
            onToggle={() => toggle("password")}
            monoTitle={false}
            typeBadge={noAuth ? noAuthBadge : undefined}
          >
            <ReadOnlyProvider readOnly={authReadOnly}>
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
            </ReadOnlyProvider>
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
            helpRef="service-settings#tls-configuration"
            dark={dark}
            expanded={isExpanded("tls")}
            onToggle={() => toggle("tls")}
            monoTitle={false}
            typeBadge={savedTlsEnabled ? { text: "enabled", title: "saved configuration serves HTTPS", helpRef: "service-settings#tls-configuration" } : undefined}
            typeBadgeAccent={savedTlsEnabled}
          >
            <div className="flex flex-col gap-4">
              {savedTlsCertMissing && (
                <p className="text-[0.8em] text-severity-warn">
                  The saved default certificate no longer exists. HTTPS cannot be served from it, and the next save turns TLS off.
                </p>
              )}
              <FormField
                label="Default certificate"
                description="Certificate used for HTTPS. Set in Certificates tab."
                helpRef={DEFAULT_CERT_HELP_REF}
                dark={dark}
              >
                <EligibilitySelect
                  value={s.tlsDefaultCert}
                  onChange={set("tlsDefaultCert")}
                  options={certs
                    .map((cert) => ({ value: encode(cert.id), label: cert.name || encode(cert.id) }))
                    .sort((a, b) => a.label.localeCompare(b.label))}
                  placeholder="None"
                  emptyMessage="No certificates stored — add one in the Certificates tab."
                  helpRef={DEFAULT_CERT_HELP_REF}
                  dark={dark}
                />
              </FormField>

              <FormField
                label="Enable TLS (HTTPS)"
                description="Serve HTTPS when a default certificate is set"
                disabledReason={hasCert ? undefined : "Requires a default certificate."}
                dark={dark}
              >
                <Checkbox
                  checked={s.tlsEnabled}
                  onChange={set("tlsEnabled")}
                  dark={dark}
                />
              </FormField>
              <FormField
                label="HTTPS port"
                description="Port for the HTTPS listener. Leave empty for HTTP port + 1."
                disabledReason={tlsGateReason}
                dark={dark}
              >
                <NumberInput
                  value={s.httpsPort}
                  onChange={set("httpsPort")}
                  placeholder="auto"
                  dark={dark}
                />
              </FormField>
              <FormField
                label="Redirect HTTP to HTTPS"
                description="Redirect plain HTTP requests to HTTPS"
                disabledReason={tlsGateReason}
                dark={dark}
              >
                <Checkbox
                  checked={s.httpToHttpsRedirect}
                  onChange={set("httpToHttpsRedirect")}
                  dark={dark}
                />
              </FormField>
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
                description="How often each node broadcasts its full stats payload (vault stats, route stats, alerts) to peers. Lower values give fresher data but increase network traffic."
                helpRef="service-settings#broadcasting"
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
                    return <p className="text-[0.75em] text-severity-warn mt-1">Invalid duration format</p>;
                  if (secs !== null && secs < 1)
                    return <p className="text-[0.75em] text-severity-warn mt-1">Must be at least 1 second</p>;
                  return null;
                })()}
              </FormField>
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="Pipeline"
            dark={dark}
            expanded={isExpanded("pipeline")}
            onToggle={() => toggle("pipeline")}
            monoTitle={false}
          >
            <div className="flex flex-col gap-4">
              <FormField
                label="Backlog Budget"
                description="Per-vault budget for the pipeline backlog (completed segments awaiting chunking). At the budget, new records for that vault are refused cluster-wide — retryable backpressure — until chunking drains it. Bounds the backlog before disk pressure; the disk-space guard remains the backstop. Applies to every vault. Leave empty for unbounded."
                helpRef="service-settings#pipeline"
                dark={dark}
              >
                <TextInput
                  value={s.pipelineBacklogMax}
                  onChange={set("pipelineBacklogMax")}
                  placeholder=""
                  dark={dark}
                  mono
                  examples={["2GB", "10GB", "50GB"]}
                />
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
                Discard
              </Button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
