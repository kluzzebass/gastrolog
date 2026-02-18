import { useEffect, useMemo, useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { Dialog } from "../Dialog";
import {
  EyeIcon,
  EyeOffIcon,
  CertIcon,
  ServiceIcon,
  FilterIcon,
  StoresIcon,
  IngestersIcon,
  RetentionIcon,
  PolicyIcon,
  UsersIcon,
} from "../icons";
import { StoresSettings } from "./StoresSettings";
import { IngestersSettings } from "./IngestersSettings";
import { CertificatesSettings } from "./CertificatesSettings";
import { FiltersSettings } from "./FiltersSettings";
import { PoliciesSettings } from "./PoliciesSettings";
import { RetentionPoliciesSettings } from "./RetentionPoliciesSettings";
import { UsersSettings } from "./UsersSettings";
import {
  useServerConfig,
  usePutServerConfig,
  useCertificates,
  JWT_KEEP,
} from "../../api/hooks/useConfig";
import { useToast } from "../Toast";
import { FormField, TextInput, NumberInput } from "./FormField";
import { PrimaryButton, GhostButton } from "./Buttons";
import { Checkbox } from "./Checkbox";
import { HelpButton } from "../HelpButton";

export type SettingsTab =
  | "service"
  | "certificates"
  | "stores"
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
  { id: "users", label: "Users", icon: UsersIcon, adminOnly: true, helpTopicId: "user-management" },
  { id: "ingesters", label: "Ingesters", icon: IngestersIcon, helpTopicId: "ingesters" },
  { id: "filters", label: "Filters", icon: FilterIcon, helpTopicId: "routing" },
  { id: "policies", label: "Rotation Policies", icon: PolicyIcon, helpTopicId: "policy-rotation" },
  { id: "retention", label: "Retention Policies", icon: RetentionIcon, helpTopicId: "policy-retention" },
  { id: "stores", label: "Stores", icon: StoresIcon, helpTopicId: "storage-engines" },
];

export function SettingsDialog({
  dark,
  tab,
  onTabChange,
  onClose,
  isAdmin,
}: SettingsDialogProps) {
  const c = useThemeClass(dark);
  const tabs = useMemo(
    () => allTabs.filter((t) => !t.adminOnly || isAdmin),
    [isAdmin],
  );

  return (
    <Dialog onClose={onClose} ariaLabel="Settings" dark={dark}>
      <div className="flex h-full overflow-hidden">
        <nav
          className={`w-48 shrink-0 border-r overflow-y-auto app-scroll p-3 ${c("border-ink-border", "border-light-border")}`}
        >
          <h2
            className={`text-[0.75em] uppercase tracking-wider font-medium mb-3 px-2 ${c("text-text-ghost", "text-light-text-ghost")}`}
          >
            Settings
          </h2>
          {tabs.map(({ id, label, icon: Icon, helpTopicId }) => (
            <div key={id} className="flex items-center mb-0.5">
              <button
                onClick={() => onTabChange(id)}
                className={`flex items-center gap-2 flex-1 text-left px-2 py-1.5 rounded text-[0.85em] transition-colors ${
                  tab === id
                    ? "bg-copper/15 text-copper font-medium"
                    : c(
                        "text-text-muted hover:text-text-bright hover:bg-ink-hover",
                        "text-light-text-muted hover:text-light-text-bright hover:bg-light-hover",
                      )
                }`}
              >
                <Icon className="w-3.5 h-3.5 shrink-0" />
                {label}
              </button>
              {tab === id && helpTopicId && <HelpButton topicId={helpTopicId} />}
            </div>
          ))}
        </nav>

        <div className="flex-1 overflow-y-auto app-scroll p-5 pt-10">
          {tab === "service" && <ServiceSettings dark={dark} />}
          {tab === "certificates" && <CertificatesSettings dark={dark} />}
          {tab === "users" && <UsersSettings dark={dark} />}
          {tab === "ingesters" && <IngestersSettings dark={dark} />}
          {tab === "filters" && <FiltersSettings dark={dark} />}
          {tab === "policies" && <PoliciesSettings dark={dark} />}
          {tab === "retention" && <RetentionPoliciesSettings dark={dark} />}
          {tab === "stores" && <StoresSettings dark={dark} />}
        </div>

        <button
          onClick={onClose}
          aria-label="Close"
          className={`absolute top-3 right-3 w-7 h-7 flex items-center justify-center rounded text-lg leading-none transition-colors ${c("text-text-muted hover:text-text-bright", "text-light-text-muted hover:text-light-text-bright")}`}
        >
          &times;
        </button>
      </div>
    </Dialog>
  );
}

function ServiceSettings({ dark }: { dark: boolean }) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useServerConfig();
  const { data: certData } = useCertificates();
  const putConfig = usePutServerConfig();
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
  const [initialized, setInitialized] = useState(false);
  const [showSecret, setShowSecret] = useState(false);

  const certs = certData?.certificates ?? [];
  const certIds = certs.map((c) => c.id);
  const _certDisplayName = (id: string) => certs.find((c) => c.id === id)?.name || id;

  useEffect(() => {
    if (data && !initialized) {
      setTokenDuration(data.tokenDuration);
      setJwtSecret(data.jwtSecretConfigured ? JWT_KEEP : "");
      setMinPwLen(
        data.minPasswordLength ? String(data.minPasswordLength) : "8",
      );
      setMaxJobs(data.maxConcurrentJobs ? String(data.maxConcurrentJobs) : "4");
      setTlsDefaultCert(data.tlsDefaultCert ?? "");
      setTlsEnabled(data.tlsEnabled ?? false);
      setHttpToHttpsRedirect(data.httpToHttpsRedirect ?? false);
      setHttpsPort(data.httpsPort ?? "");
      setRequireMixedCase(data.requireMixedCase ?? false);
      setRequireDigit(data.requireDigit ?? false);
      setRequireSpecial(data.requireSpecial ?? false);
      setMaxConsecutiveRepeats(data.maxConsecutiveRepeats ? String(data.maxConsecutiveRepeats) : "0");
      setForbidAnimalNoise(data.forbidAnimalNoise ?? false);
      setInitialized(true);
    }
  }, [data, initialized]);

  const dirty =
    initialized &&
    data &&
    (tokenDuration !== data.tokenDuration ||
      (jwtSecret !== JWT_KEEP && jwtSecret !== "") ||
      minPwLen !== String(data.minPasswordLength || 8) ||
      maxJobs !== String(data.maxConcurrentJobs || 4) ||
      tlsDefaultCert !== (data.tlsDefaultCert ?? "") ||
      tlsEnabled !== (data.tlsEnabled ?? false) ||
      httpToHttpsRedirect !== (data.httpToHttpsRedirect ?? false) ||
      httpsPort !== (data.httpsPort ?? "") ||
      requireMixedCase !== (data.requireMixedCase ?? false) ||
      requireDigit !== (data.requireDigit ?? false) ||
      requireSpecial !== (data.requireSpecial ?? false) ||
      maxConsecutiveRepeats !== String(data.maxConsecutiveRepeats || 0) ||
      forbidAnimalNoise !== (data.forbidAnimalNoise ?? false));

  const handleSave = async () => {
    try {
      await putConfig.mutateAsync({
        tokenDuration,
        jwtSecret: jwtSecret === JWT_KEEP ? JWT_KEEP : jwtSecret,
        minPasswordLength: parseInt(minPwLen, 10) || 8,
        maxConcurrentJobs: parseInt(maxJobs, 10) || 4,
        tlsDefaultCert,
        tlsEnabled: certIds.includes(tlsDefaultCert) ? tlsEnabled : false,
        httpToHttpsRedirect:
          certIds.includes(tlsDefaultCert) ? httpToHttpsRedirect : false,
        httpsPort,
        requireMixedCase,
        requireDigit,
        requireSpecial,
        maxConsecutiveRepeats: parseInt(maxConsecutiveRepeats, 10) || 0,
        forbidAnimalNoise,
      });
      addToast("Server configuration updated", "info");
    } catch (err: any) {
      addToast(err.message ?? "Failed to update server configuration", "error");
    }
  };

  const handleReset = () => {
    if (data) {
      setTokenDuration(data.tokenDuration);
      setJwtSecret(data.jwtSecretConfigured ? JWT_KEEP : "");
      setMinPwLen(
        data.minPasswordLength ? String(data.minPasswordLength) : "8",
      );
      setMaxJobs(data.maxConcurrentJobs ? String(data.maxConcurrentJobs) : "4");
      setTlsDefaultCert(data.tlsDefaultCert ?? "");
      setTlsEnabled(data.tlsEnabled ?? false);
      setHttpToHttpsRedirect(data.httpToHttpsRedirect ?? false);
      setHttpsPort(data.httpsPort ?? "");
      setRequireMixedCase(data.requireMixedCase ?? false);
      setRequireDigit(data.requireDigit ?? false);
      setRequireSpecial(data.requireSpecial ?? false);
      setMaxConsecutiveRepeats(data.maxConsecutiveRepeats ? String(data.maxConsecutiveRepeats) : "0");
      setForbidAnimalNoise(data.forbidAnimalNoise ?? false);
    }
  };

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
        <div className="flex flex-col gap-8 max-w-lg">
          {/* ── Authentication ── */}
          <section className="flex flex-col gap-5">
            <h3 className={`text-[0.75em] uppercase tracking-wider font-medium pb-1 border-b ${c("text-text-ghost border-ink-border", "text-light-text-ghost border-light-border")}`}>
              Authentication
            </h3>

            <FormField
              label="Token Duration"
              description="How long authentication tokens remain valid. Use Go duration syntax, e.g. 168h, 720h, 24h."
              dark={dark}
            >
              <TextInput
                value={tokenDuration}
                onChange={setTokenDuration}
                placeholder="168h"
                dark={dark}
                mono
              />
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
                  placeholder={data?.jwtSecretConfigured ? "•••••••• (paste to replace)" : "Set JWT secret"}
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
          </section>

          {/* ── Password Policy ── */}
          <section className="flex flex-col gap-5">
            <h3 className={`text-[0.75em] uppercase tracking-wider font-medium pb-1 border-b ${c("text-text-ghost border-ink-border", "text-light-text-ghost border-light-border")}`}>
              Password Policy
            </h3>

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
              <Checkbox checked={forbidAnimalNoise} onChange={setForbidAnimalNoise} label="Forbid animal noises (moo, woof, meow, …)" dark={dark} />
            </div>
          </section>

          {/* ── Scheduler ── */}
          <section className="flex flex-col gap-5">
            <h3 className={`text-[0.75em] uppercase tracking-wider font-medium pb-1 border-b ${c("text-text-ghost border-ink-border", "text-light-text-ghost border-light-border")}`}>
              Scheduler
            </h3>

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
              />
            </FormField>
          </section>

          {/* ── TLS ── */}
          <section className="flex flex-col gap-5">
            <h3 className={`text-[0.75em] uppercase tracking-wider font-medium pb-1 border-b ${c("text-text-ghost border-ink-border", "text-light-text-ghost border-light-border")}`}>
              TLS
            </h3>

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
                <option value="">— none —</option>
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
          </section>

          <div className="flex gap-2 mt-1">
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

