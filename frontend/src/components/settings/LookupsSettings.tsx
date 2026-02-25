import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import {
  useServerConfig,
  usePutServerConfig,
  MAXMIND_KEEP,
} from "../../api/hooks/useConfig";
import { useToast } from "../Toast";
import { FormField, TextInput } from "./FormField";
import { Checkbox } from "./Checkbox";
import { PrimaryButton, GhostButton } from "./Buttons";
import { ExpandableCard } from "./ExpandableCard";

// eslint-disable-next-line sonarjs/cognitive-complexity -- inherently complex settings form with multiple expandable cards and dirty tracking
export function LookupsSettings({ dark }: Readonly<{ dark: boolean }>) {
  const c = useThemeClass(dark);
  const { data, isLoading } = useServerConfig();
  const putConfig = usePutServerConfig();
  const { addToast } = useToast();

  const [geoipDbPath, setGeoipDbPath] = useState("");
  const [asnDbPath, setAsnDbPath] = useState("");
  const [autoDownload, setAutoDownload] = useState(false);
  const [accountId, setAccountId] = useState("");
  const [licenseKey, setLicenseKey] = useState("");
  const [initialized, setInitialized] = useState(false);

  const [expandedCards, setExpandedCards] = useState<Record<string, boolean>>({
    maxmind: true,
    geoip: false,
    asn: false,
  });

  const toggle = (key: string) =>
    setExpandedCards((prev) => ({ ...prev, [key]: !prev[key] }));

  if (data && !initialized) {
    setGeoipDbPath(data.geoipDbPath);
    setAsnDbPath(data.asnDbPath);
    setAutoDownload(data.maxmindAutoDownload);
    setAccountId("");
    setLicenseKey("");
    setInitialized(true);
  }

  const dirty =
    initialized &&
    data &&
    (geoipDbPath !== data.geoipDbPath ||
      asnDbPath !== data.asnDbPath ||
      autoDownload !== data.maxmindAutoDownload ||
      accountId !== "" ||
      licenseKey !== "");

  const handleSave = async () => {
    try {
      await putConfig.mutateAsync({
        geoipDbPath,
        asnDbPath,
        maxmindAutoDownload: autoDownload,
        maxmindAccountId: accountId || undefined,
        maxmindLicenseKey: licenseKey || MAXMIND_KEEP,
      });
      setAccountId("");
      setLicenseKey("");
      addToast("Lookup configuration updated", "info");
    } catch (err: any) {
      const msg = err.message ?? "Failed to update lookup configuration";
      addToast(msg, "error");
    }
  };

  const handleReset = () => {
    if (data) {
      setGeoipDbPath(data.geoipDbPath);
      setAsnDbPath(data.asnDbPath);
      setAutoDownload(data.maxmindAutoDownload);
      setAccountId("");
      setLicenseKey("");
    }
  };

  let geoipBadge: string | undefined;
  if (geoipDbPath) {
    geoipBadge = "manual path";
  } else if (autoDownload) {
    geoipBadge = "auto";
  }

  let asnBadge: string | undefined;
  if (asnDbPath) {
    asnBadge = "manual path";
  } else if (autoDownload) {
    asnBadge = "auto";
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <h2
          className={`font-display text-[1.4em] font-semibold ${c("text-text-bright", "text-light-text-bright")}`}
        >
          Lookups
        </h2>
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
            id="MaxMind Auto-Download"
            dark={dark}
            expanded={!!expandedCards.maxmind}
            onToggle={() => toggle("maxmind")}
            monoTitle={false}
            typeBadge={autoDownload ? "enabled" : undefined}
            typeBadgeAccent={true}
            status={
              data?.maxmindLicenseConfigured ? (
                <span className="text-green-500 text-[0.8em]">&#10003;</span>
              ) : undefined
            }
          >
            <div className="flex flex-col gap-4">
              <Checkbox
                checked={autoDownload}
                onChange={setAutoDownload}
                label="Enable automatic database downloads"
                dark={dark}
              />

              <p
                className={`text-[0.8em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
              >
                When enabled, GeoLite2-City and GeoLite2-ASN databases are
                downloaded automatically and updated twice weekly (Tue/Fri at
                03:00). Requires a free{" "}
                <a
                  href="https://www.maxmind.com/en/geolite2/signup"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-copper hover:underline"
                >
                  MaxMind account
                </a>
                .
              </p>

              <FormField
                label="Account ID"
                description="Your MaxMind account ID (numeric)."
                dark={dark}
              >
                <TextInput
                  value={accountId}
                  onChange={setAccountId}
                  placeholder={
                    data?.maxmindLicenseConfigured
                      ? "(configured — leave blank to keep)"
                      : "123456"
                  }
                  dark={dark}
                  mono
                />
              </FormField>

              <FormField
                label="License Key"
                description="Your MaxMind license key."
                dark={dark}
              >
                <PasswordInput
                  value={licenseKey}
                  onChange={setLicenseKey}
                  placeholder={
                    data?.maxmindLicenseConfigured
                      ? "(configured — leave blank to keep)"
                      : "Enter license key"
                  }
                  dark={dark}
                />
              </FormField>

              {data?.maxmindLastUpdate && (
                <div
                  className={`text-[0.8em] ${c("text-text-ghost", "text-light-text-ghost")}`}
                >
                  Last updated:{" "}
                  {new Date(data.maxmindLastUpdate).toLocaleString()}
                </div>
              )}
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="GeoIP"
            dark={dark}
            expanded={!!expandedCards.geoip}
            onToggle={() => toggle("geoip")}
            monoTitle={false}
            typeBadge={geoipBadge}
            typeBadgeAccent={!geoipDbPath && autoDownload}
          >
            <div className="flex flex-col gap-4">
              <p
                className={`text-[0.8em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
              >
                Enriches IP addresses with country, city, and coordinates via{" "}
                <span className="font-mono">| lookup geoip</span>.
              </p>

              <FormField
                label="Manual MMDB Path"
                description={
                  autoDownload
                    ? "Overrides the auto-downloaded GeoLite2-City database. Leave blank to use auto-download."
                    : "Path to a GeoLite2-City or GeoIP2-City .mmdb file. Hot-reloaded on changes."
                }
                dark={dark}
              >
                <TextInput
                  value={geoipDbPath}
                  onChange={setGeoipDbPath}
                  placeholder={
                    autoDownload
                      ? "(using auto-downloaded GeoLite2-City)"
                      : "path/to/GeoLite2-City.mmdb"
                  }
                  dark={dark}
                  mono
                />
              </FormField>
            </div>
          </ExpandableCard>

          <ExpandableCard
            id="ASN"
            dark={dark}
            expanded={!!expandedCards.asn}
            onToggle={() => toggle("asn")}
            monoTitle={false}
            typeBadge={asnBadge}
            typeBadgeAccent={!asnDbPath && autoDownload}
          >
            <div className="flex flex-col gap-4">
              <p
                className={`text-[0.8em] leading-relaxed ${c("text-text-muted", "text-light-text-muted")}`}
              >
                Enriches IP addresses with AS number and organization via{" "}
                <span className="font-mono">| lookup asn</span>.
              </p>

              <FormField
                label="Manual MMDB Path"
                description={
                  autoDownload
                    ? "Overrides the auto-downloaded GeoLite2-ASN database. Leave blank to use auto-download."
                    : "Path to a GeoLite2-ASN or GeoIP2-ISP .mmdb file. Hot-reloaded on changes."
                }
                dark={dark}
              >
                <TextInput
                  value={asnDbPath}
                  onChange={setAsnDbPath}
                  placeholder={
                    autoDownload
                      ? "(using auto-downloaded GeoLite2-ASN)"
                      : "path/to/GeoLite2-ASN.mmdb"
                  }
                  dark={dark}
                  mono
                />
              </FormField>
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

function PasswordInput({
  value,
  onChange,
  placeholder,
  dark,
}: Readonly<{
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  return (
    <input
      type="password"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      autoComplete="off"
      className={`px-2.5 py-1.5 text-[0.85em] font-mono border rounded focus:outline-none transition-colors ${c(
        "bg-ink-surface border-ink-border text-text-bright placeholder:text-text-ghost focus:border-copper-dim",
        "bg-light-surface border-light-border text-light-text-bright placeholder:text-light-text-ghost focus:border-copper",
      )}`}
    />
  );
}
