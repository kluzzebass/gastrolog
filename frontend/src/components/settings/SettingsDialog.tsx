import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { Dialog } from "../Dialog";
import {
  EyeIcon,
  EyeOffIcon,
  CertIcon,
  ServiceIcon,
  FilterIcon,
  VaultsIcon,
  IngestersIcon,
  RetentionIcon,
  PolicyIcon,
  UsersIcon,
  LookupIcon,
} from "../icons";
import { VaultsSettings } from "./VaultsSettings";
import { IngestersSettings } from "./IngestersSettings";
import { CertificatesSettings } from "./CertificatesSettings";
import { FiltersSettings } from "./FiltersSettings";
import { PoliciesSettings } from "./PoliciesSettings";
import { RetentionPoliciesSettings } from "./RetentionPoliciesSettings";
import { UsersSettings } from "./UsersSettings";
import { LookupsSettings } from "./LookupsSettings";
import {
  useSettings,
  usePutSettings,
  usePutNodeConfig,
  useCertificates,
  JWT_KEEP,
} from "../../api/hooks/useConfig";
import { useToast } from "../Toast";
import { FormField, TextInput, NumberInput } from "./FormField";
import { PrimaryButton, GhostButton } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { HelpButton } from "../HelpButton";
import { ExpandableCard } from "./ExpandableCard";

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

export type SettingsTab =
  | "service"
  | "certificates"
  | "lookups"
  | "vaults"
  | "ingesters"
  | "filters"
  | "policies"
  | "retention"
  | "users";

interface SettingsDialogProps {
  dark: boolean;
  tab: SettingsTab;
  onTabChange: (tab: SettingsTab) => void;
  onClose: () => void;
  isAdmin?: boolean;
  noAuth?: boolean;
}

type TabDef = {
  id: SettingsTab;
  label: string;
  icon: (p: { className?: string }) => React.ReactNode;
  adminOnly?: boolean;
  helpTopicId?: string;
};

const allTabs: TabDef[] = [
  { id: "service", label: "Service", icon: ServiceIcon, helpTopicId: "service-settings" },
  { id: "certificates", label: "Certificates", icon: CertIcon, helpTopicId: "certificates" },
  { id: "lookups", label: "Lookups", icon: LookupIcon },
  { id: "users", label: "Users", icon: UsersIcon, adminOnly: true, helpTopicId: "user-management" },
  { id: "ingesters", label: "Ingesters", icon: IngestersIcon, helpTopicId: "ingesters" },
  { id: "filters", label: "Filters", icon: FilterIcon, helpTopicId: "routing" },
  { id: "policies", label: "Rotation Policies", icon: PolicyIcon, helpTopicId: "policy-rotation" },
  { id: "retention", label: "Retention Policies", icon: RetentionIcon, helpTopicId: "policy-retention" },
  { id: "vaults", label: "Vaults", icon: VaultsIcon, helpTopicId: "storage-engines" },
];

export function SettingsDialog({
  dark,
  tab,
  onTabChange,
  onClose,
  isAdmin,
  noAuth,
}: Readonly<SettingsDialogProps>) {
  const c = useThemeClass(dark);
  const tabs = allTabs.filter((t) => !t.adminOnly || isAdmin);
  const [expandTarget, setExpandTarget] = useState<string | null>(null);

  const navigateTo = (targetTab: SettingsTab, entityName?: string) => {
    onTabChange(targetTab);
    setExpandTarget(entityName ?? null);
  };

  const clearExpandTarget = () => setExpandTarget(null);

  return (
    <Dialog onClose={onClose} ariaLabel="Settings" dark={dark}>
      <div className="flex h-full overflow-hidden">
        <nav
          className={`min-w-fit shrink-0 border-r overflow-y-auto app-scroll p-3 ${c("border-ink-border", "border-light-border")}`}
        >
          <h2
            className={`text-[0.75em] uppercase tracking-wider font-medium mb-3 px-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Settings
          </h2>
          {tabs.map(({ id, label, icon: Icon }) => (
            <button
              key={id}
              onClick={() => onTabChange(id)}
              className={`flex items-center gap-2 w-full text-left px-2 py-1.5 mb-0.5 rounded text-[0.85em] transition-colors ${
                tab === id
                  ? "bg-copper/15 text-copper"
                  : c(
                      "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                      "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                    )
              }`}
            >
              <Icon className="w-3.5 h-3.5 shrink-0" />
              <span className="whitespace-nowrap">{label}</span>
            </button>
          ))}
        </nav>

        <div className="flex-1 overflow-y-auto app-scroll p-5">
          {tab === "service" && <ServiceSettings dark={dark} noAuth={noAuth} />}
          {tab === "certificates" && <CertificatesSettings dark={dark} />}
          {tab === "lookups" && <LookupsSettings dark={dark} />}
          {tab === "users" && <UsersSettings dark={dark} noAuth={noAuth} />}
          {tab === "ingesters" && <IngestersSettings dark={dark} />}
          {tab === "filters" && <FiltersSettings dark={dark} onNavigateTo={navigateTo} />}
          {tab === "policies" && <PoliciesSettings dark={dark} onNavigateTo={navigateTo} />}
          {tab === "retention" && <RetentionPoliciesSettings dark={dark} onNavigateTo={navigateTo} />}
          {tab === "vaults" && <VaultsSettings dark={dark} expandTarget={expandTarget} onExpandTargetConsumed={clearExpandTarget} />}
        </div>
      </div>
    </Dialog>
  );
}

// eslint-disable-next-line sonarjs/cognitive-complexity -- inherently complex settings form with many fields, cards, and dirty tracking
function ServiceSettings({ dark, noAuth }: Readonly<{ dark: boolean; noAuth?: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useSettings();
  const { data: certData } = useCertificates();
  const putConfig = usePutSettings();
  const putNodeConfig = usePutNodeConfig();
  const { addToast } = useToast();

  const [tokenDuration, setTokenDuration] = useState("");
  const [jwtSecret, setJwtSecret] = useState("");
  const [minPwLen, setMinPwLen] = useState("");
  const [maxJobs, setMaxJobs] = useState("");
  const [tlsDefaultCert, setTlsDefaultCert] = useState("");
  const [tlsEnabled, setTlsEnabled] = useState(false);
  const [httpToHttpsRedirect, setHttpToHttpsRedirect] = useState(false);
  const [httpsPort, setHttpsPort] = useState("");
  const [requireMixedCase, setRequireMixedCase] = useState(false);
  const [requireDigit, setRequireDigit] = useState(false);
  const [requireSpecial, setRequireSpecial] = useState(false);
  const [maxConsecutiveRepeats, setMaxConsecutiveRepeats] = useState("");
  const [forbidAnimalNoise, setForbidAnimalNoise] = useState(false);
  const [refreshTokenDuration, setRefreshTokenDuration] = useState("");
  const [maxFollowDuration, setMaxFollowDuration] = useState("");
  const [queryTimeout, setQueryTimeout] = useState("");
  const [maxResultCount, setMaxResultCount] = useState("");
  const [nodeName, setNodeName] = useState("");
  const [initialized, setInitialized] = useState(false);
  const [showSecret, setShowSecret] = useState(false);

  const certs = certData?.certificates ?? [];
  const certIdSet = new Set(certs.map((c) => c.id));
  const _certDisplayName = (id: string) => certs.find((c) => c.id === id)?.name || id;

  if (data && !initialized) {
    const auth = data.auth;
    const pp = auth?.passwordPolicy;
    const query = data.query;
    const sched = data.scheduler;
    const tls = data.tls;
    setTokenDuration(auth?.tokenDuration ?? "");
    setJwtSecret(auth?.jwtSecretConfigured ? JWT_KEEP : "");
    setMinPwLen(pp?.minLength ? String(pp.minLength) : "8");
    setMaxJobs(sched?.maxConcurrentJobs ? String(sched.maxConcurrentJobs) : "4");
    setTlsDefaultCert(tls?.defaultCert ?? "");
    setTlsEnabled(tls?.enabled ?? false);
    setHttpToHttpsRedirect(tls?.httpToHttpsRedirect ?? false);
    setHttpsPort(tls?.httpsPort ?? "");
    setRequireMixedCase(pp?.requireMixedCase ?? false);
    setRequireDigit(pp?.requireDigit ?? false);
    setRequireSpecial(pp?.requireSpecial ?? false);
    setMaxConsecutiveRepeats(pp?.maxConsecutiveRepeats ? String(pp.maxConsecutiveRepeats) : "0");
    setForbidAnimalNoise(pp?.forbidAnimalNoise ?? false);
    setRefreshTokenDuration(auth?.refreshTokenDuration ?? "");
    setMaxFollowDuration(query?.maxFollowDuration ?? "");
    setQueryTimeout(query?.timeout ?? "");
    setMaxResultCount(query?.maxResultCount ? String(query.maxResultCount) : "10000");
    setNodeName(data.nodeName);
    setInitialized(true);
  }

  const dirty =
    initialized &&
    data &&
    (tokenDuration !== (data.auth?.tokenDuration ?? "") ||
      (jwtSecret !== JWT_KEEP && jwtSecret !== "") ||
      minPwLen !== String(data.auth?.passwordPolicy?.minLength || 8) ||
      maxJobs !== String(data.scheduler?.maxConcurrentJobs || 4) ||
      tlsDefaultCert !== (data.tls?.defaultCert ?? "") ||
      tlsEnabled !== (data.tls?.enabled ?? false) ||
      httpToHttpsRedirect !== (data.tls?.httpToHttpsRedirect ?? false) ||
      httpsPort !== (data.tls?.httpsPort ?? "") ||
      requireMixedCase !== (data.auth?.passwordPolicy?.requireMixedCase ?? false) ||
      requireDigit !== (data.auth?.passwordPolicy?.requireDigit ?? false) ||
      requireSpecial !== (data.auth?.passwordPolicy?.requireSpecial ?? false) ||
      maxConsecutiveRepeats !== String(data.auth?.passwordPolicy?.maxConsecutiveRepeats || 0) ||
      forbidAnimalNoise !== (data.auth?.passwordPolicy?.forbidAnimalNoise ?? false) ||
      refreshTokenDuration !== (data.auth?.refreshTokenDuration ?? "") ||
      maxFollowDuration !== (data.query?.maxFollowDuration ?? "") ||
      queryTimeout !== (data.query?.timeout ?? "") ||
      maxResultCount !== String(data.query?.maxResultCount || 10000));

  const handleSave = async () => {
    const hasCert = certIdSet.has(tlsDefaultCert);
    const effectiveTls = hasCert ? tlsEnabled : false;
    const effectiveRedirect = hasCert ? httpToHttpsRedirect : false;
    const effectiveJwtSecret = jwtSecret === JWT_KEEP ? JWT_KEEP : jwtSecret;
    const effectiveMinPwLen = parseInt(minPwLen, 10) || 8;
    const effectiveMaxJobs = parseInt(maxJobs, 10) || 4;
    const effectiveMaxRepeats = parseInt(maxConsecutiveRepeats, 10) || 0;
    const effectiveMaxResultCount = parseInt(maxResultCount, 10) || 0;
    try {
      await putConfig.mutateAsync({
        auth: {
          tokenDuration,
          jwtSecret: effectiveJwtSecret,
          refreshTokenDuration,
          passwordPolicy: {
            minLength: effectiveMinPwLen,
            requireMixedCase,
            requireDigit,
            requireSpecial,
            maxConsecutiveRepeats: effectiveMaxRepeats,
            forbidAnimalNoise,
          },
        },
        query: {
          timeout: queryTimeout,
          maxFollowDuration,
          maxResultCount: effectiveMaxResultCount,
        },
        scheduler: {
          maxConcurrentJobs: effectiveMaxJobs,
        },
        tls: {
          defaultCert: tlsDefaultCert,
          enabled: effectiveTls,
          httpToHttpsRedirect: effectiveRedirect,
          httpsPort,
        },
      });
      addToast("Server configuration updated", "info");
    } catch (err: any) {
      const msg = err.message ?? "Failed to update server configuration";
      addToast(msg, "error");
    }
  };

  const handleReset = () => {
    if (data) {
      const auth = data.auth;
      const pp = auth?.passwordPolicy;
      const query = data.query;
      const sched = data.scheduler;
      const tls = data.tls;
      setTokenDuration(auth?.tokenDuration ?? "");
      setJwtSecret(auth?.jwtSecretConfigured ? JWT_KEEP : "");
      setMinPwLen(pp?.minLength ? String(pp.minLength) : "8");
      setMaxJobs(sched?.maxConcurrentJobs ? String(sched.maxConcurrentJobs) : "4");
      setTlsDefaultCert(tls?.defaultCert ?? "");
      setTlsEnabled(tls?.enabled ?? false);
      setHttpToHttpsRedirect(tls?.httpToHttpsRedirect ?? false);
      setHttpsPort(tls?.httpsPort ?? "");
      setRequireMixedCase(pp?.requireMixedCase ?? false);
      setRequireDigit(pp?.requireDigit ?? false);
      setRequireSpecial(pp?.requireSpecial ?? false);
      setMaxConsecutiveRepeats(pp?.maxConsecutiveRepeats ? String(pp.maxConsecutiveRepeats) : "0");
      setForbidAnimalNoise(pp?.forbidAnimalNoise ?? false);
      setRefreshTokenDuration(auth?.refreshTokenDuration ?? "");
      setMaxFollowDuration(query?.maxFollowDuration ?? "");
      setQueryTimeout(query?.timeout ?? "");
      setMaxResultCount(query?.maxResultCount ? String(query.maxResultCount) : "10000");
      setNodeName(data.nodeName);
    }
  };

  const [expandedCards, setExpandedCards] = useState<Record<string, boolean>>({
    node: false,
    auth: true,
    password: false,
    scheduler: false,
    query: false,
    tls: false,
  });

  const toggle = (key: string) =>
    setExpandedCards((prev) => ({ ...prev, [key]: !prev[key] }));

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-2">
          <h2
            className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
          >
            Service
          </h2>
          <HelpButton topicId="service-settings" />
        </div>
      </div>

      {isLoading ? (
        <div
          className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Loading...
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          <ExpandableCard
            id="Node"
            dark={dark}
            expanded={!!expandedCards.node}
            onToggle={() => toggle("node")}
            monoTitle={false}
          >
            <div className="flex flex-col gap-4">
              <FormField label="Node ID" description="Unique identifier for this node. Auto-generated, read-only." dark={dark}>
                <TextInput value={data?.nodeId ?? ""} onChange={() => {}} dark={dark} mono disabled />
              </FormField>
              <FormField label="Node Name" description="Human-readable name for this node. Visible to all nodes in the cluster." dark={dark}>
                <TextInput value={nodeName} onChange={setNodeName} placeholder="e.g. witty-hamster" dark={dark} mono />
              </FormField>
              <div className="flex gap-2">
                <PrimaryButton
                  onClick={async () => {
                    if (!nodeName.trim()) {
                      addToast("Node name must not be empty", "error");
                      return;
                    }
                    try {
                      await putNodeConfig.mutateAsync({ id: data?.nodeId ?? "", name: nodeName.trim() });
                      addToast("Node name updated", "info");
                    } catch (err: any) {
                      addToast(err.message ?? "Failed to update node name", "error");
                    }
                  }}
                  disabled={!initialized || nodeName === (data?.nodeName ?? "") || putNodeConfig.isPending}
                >
                  {putNodeConfig.isPending ? "Saving..." : "Save"}
                </PrimaryButton>
                {nodeName !== (data?.nodeName ?? "") && (
                  <GhostButton onClick={() => setNodeName(data?.nodeName ?? "")} dark={dark}>
                    Reset
                  </GhostButton>
                )}
              </div>
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="Authentication"
            dark={dark}
            expanded={!!expandedCards.auth}
            onToggle={() => toggle("auth")}
            monoTitle={false}
            typeBadge={noAuth ? "disabled" : undefined}
          >
            <div className={`flex flex-col gap-4 ${noAuth ? "opacity-50 pointer-events-none" : ""}`}>
              <FormField
                label="Token Duration"
                description="How long access tokens remain valid. Short-lived tokens are more secure when paired with refresh tokens."
                dark={dark}
              >
                <TextInput
                  value={tokenDuration}
                  onChange={setTokenDuration}
                  placeholder="15m"
                  dark={dark}
                  mono
                  examples={["15m", "1h", "24h"]}
                />
                {(() => {
                  const secs = parseDurationSeconds(tokenDuration);
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
                  value={refreshTokenDuration}
                  onChange={setRefreshTokenDuration}
                  placeholder="168h"
                  dark={dark}
                  mono
                  examples={["24h", "168h", "720h"]}
                />
                {(() => {
                  const secs = parseDurationSeconds(refreshTokenDuration);
                  const tokenSecs = parseDurationSeconds(tokenDuration);
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
                    type={showSecret ? "text" : "password"}
                    value={jwtSecret === JWT_KEEP ? "" : jwtSecret}
                    onChange={(e) => setJwtSecret(e.target.value)}
                    placeholder={data?.auth?.jwtSecretConfigured ? "•••••••• (paste to replace)" : "Set JWT secret"}
                    className={`w-full px-2.5 py-1.5 pr-9 text-[0.85em] font-mono border rounded focus:outline-none transition-colors ${c(
                      "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
                      "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
                    )}`}
                  />
                  <button
                    type="button"
                    onClick={() => setShowSecret(!showSecret)}
                    className={`absolute right-2 top-1/2 -translate-y-1/2 transition-colors ${c(
                      "text-text-ghost hover:text-text-muted",
                      "text-light-text-ghost hover:text-light-text-muted",
                    )}`}
                  >
                    {showSecret ? (
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
            expanded={!!expandedCards.password}
            onToggle={() => toggle("password")}
            monoTitle={false}
            typeBadge={noAuth ? "disabled" : undefined}
          >
            <div className={`flex flex-col gap-4 ${noAuth ? "opacity-50 pointer-events-none" : ""}`}>
              <div className="flex items-baseline gap-4">
                <FormField
                  label="Minimum length"
                  dark={dark}
                >
                  <NumberInput
                    value={minPwLen}
                    onChange={setMinPwLen}
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
                    value={maxConsecutiveRepeats}
                    onChange={setMaxConsecutiveRepeats}
                    placeholder="0 (no limit)"
                    dark={dark}
                    min={0}
                  />
                </FormField>
              </div>

              <div className="flex flex-col gap-2.5">
                <Checkbox checked={requireMixedCase} onChange={setRequireMixedCase} label="Require mixed case (upper + lowercase)" dark={dark} />
                <Checkbox checked={requireDigit} onChange={setRequireDigit} label="Require digit (0-9)" dark={dark} />
                <Checkbox checked={requireSpecial} onChange={setRequireSpecial} label="Require special character" dark={dark} />
                <Checkbox checked={forbidAnimalNoise} onChange={setForbidAnimalNoise} label="Forbid animal noises (moo, woof, meow, ...)" dark={dark} />
              </div>
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="Scheduler"
            dark={dark}
            expanded={!!expandedCards.scheduler}
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
                  value={maxJobs}
                  onChange={setMaxJobs}
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
            expanded={!!expandedCards.query}
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
                  value={queryTimeout}
                  onChange={setQueryTimeout}
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
                  value={maxFollowDuration}
                  onChange={setMaxFollowDuration}
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
                  value={maxResultCount}
                  onChange={setMaxResultCount}
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
            expanded={!!expandedCards.tls}
            onToggle={() => toggle("tls")}
            monoTitle={false}
            typeBadge={tlsEnabled ? "enabled" : undefined}
            typeBadgeAccent={tlsEnabled}
          >
            <div className="flex flex-col gap-4">
              <FormField
                label="Default certificate"
                description="Certificate used for HTTPS. Set in Certificates tab."
                dark={dark}
              >
                <select
                  value={tlsDefaultCert}
                  onChange={(e) => setTlsDefaultCert(e.target.value)}
                  className={`w-full px-2.5 py-1.5 text-[0.85em] border rounded focus:outline-none transition-colors ${c(
                    "bg-ink-surface border-ink-border text-text-bright focus:border-copper-dim",
                    "bg-light-surface border-light-border text-light-text-bright focus:border-copper",
                  )}`}
                >
                  <option value="">-- none --</option>
                  {certs.map((cert) => (
                    <option key={cert.id} value={cert.id}>
                      {cert.name || cert.id}
                    </option>
                  ))}
                </select>
              </FormField>

              {tlsDefaultCert && (
                <>
                  <FormField
                    label="Enable TLS (HTTPS)"
                    description="Serve HTTPS when a default certificate is set"
                    dark={dark}
                  >
                    <Checkbox
                      checked={tlsEnabled}
                      onChange={setTlsEnabled}
                      dark={dark}
                    />
                  </FormField>
                  {tlsEnabled && (
                    <>
                      <FormField
                        label="HTTPS port"
                        description="Port for the HTTPS listener. Leave empty for HTTP port + 1."
                        dark={dark}
                      >
                        <input
                          type="text"
                          inputMode="numeric"
                          value={httpsPort}
                          onChange={(e) => setHttpsPort(e.target.value)}
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
                          checked={httpToHttpsRedirect}
                          onChange={setHttpToHttpsRedirect}
                          dark={dark}
                        />
                      </FormField>
                    </>
                  )}
                </>
              )}
            </div>
          </ExpandableCard>

          <div className="flex gap-2 mt-2">
            <PrimaryButton
              onClick={handleSave}
              disabled={!dirty || putConfig.isPending}
            >
              {putConfig.isPending ? "Saving..." : "Save"}
            </PrimaryButton>
            {dirty && (
              <GhostButton onClick={handleReset} dark={dark}>
                Reset
              </GhostButton>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
