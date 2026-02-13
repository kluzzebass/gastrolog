import { useMemo, useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { Dialog, DialogTabHeader } from "../Dialog";
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
};

const allTabs: TabDef[] = [
  { id: "service", label: "Service", icon: ServiceIcon },
  { id: "certificates", label: "Certificates", icon: CertIcon },
  { id: "users", label: "Users", icon: UsersIcon, adminOnly: true },
  { id: "ingesters", label: "Ingesters", icon: IngestersIcon },
  { id: "filters", label: "Filters", icon: FilterIcon },
  { id: "policies", label: "Rotation Policies", icon: PolicyIcon },
  { id: "retention", label: "Retention Policies", icon: RetentionIcon },
  { id: "stores", label: "Stores", icon: StoresIcon },
];

export function SettingsDialog({
  dark,
  tab,
  onTabChange,
  onClose,
  isAdmin,
}: SettingsDialogProps) {
  const tabs = useMemo(
    () => allTabs.filter((t) => !t.adminOnly || isAdmin),
    [isAdmin],
  );

  return (
    <Dialog onClose={onClose} ariaLabel="Settings" dark={dark}>
      <DialogTabHeader
        title="Settings"
        tabs={tabs}
        activeTab={tab}
        onTabChange={(t) => onTabChange(t as SettingsTab)}
        onClose={onClose}
        dark={dark}
      />

      <div className="flex-1 overflow-y-auto app-scroll p-5">
        {tab === "service" && <ServiceSettings dark={dark} />}
        {tab === "certificates" && <CertificatesSettings dark={dark} />}
        {tab === "users" && <UsersSettings dark={dark} />}
        {tab === "ingesters" && <IngestersSettings dark={dark} />}
        {tab === "filters" && <FiltersSettings dark={dark} />}
        {tab === "policies" && <PoliciesSettings dark={dark} />}
        {tab === "retention" && <RetentionPoliciesSettings dark={dark} />}
        {tab === "stores" && <StoresSettings dark={dark} />}
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
  const [initialized, setInitialized] = useState(false);
  const [showSecret, setShowSecret] = useState(false);

  const certNames = certData?.names ?? [];

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
      httpToHttpsRedirect !== (data.httpToHttpsRedirect ?? false));

  const handleSave = async () => {
    try {
      await putConfig.mutateAsync({
        tokenDuration,
        jwtSecret: jwtSecret === JWT_KEEP ? JWT_KEEP : jwtSecret,
        minPasswordLength: parseInt(minPwLen, 10) || 8,
        maxConcurrentJobs: parseInt(maxJobs, 10) || 4,
        tlsDefaultCert,
        tlsEnabled: certNames.includes(tlsDefaultCert) ? tlsEnabled : false,
        httpToHttpsRedirect:
          certNames.includes(tlsDefaultCert) ? httpToHttpsRedirect : false,
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
    }
  };

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Service
        </h2>
      </div>

      {isLoading ? (
        <div
          className={`text-[0.85em] ${c("text-text-ghost", "text-light-text-ghost")}`}
        >
          Loading...
        </div>
      ) : (
        <div className="flex flex-col gap-5 max-w-lg">
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

          <FormField
            label="Minimum Password Length"
            description="The minimum number of characters required for user passwords."
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

          <FormField
            label="TLS default certificate"
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
              {certNames.map((n) => (
                <option key={n} value={n}>
                  {n}
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
              )}
            </>
          )}

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

