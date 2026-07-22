import { useThemeClass } from "../../hooks/useThemeClass";
import { useNodeRegistry, useVaults } from "../../api/hooks";
import type { Storage } from "../../api/model/storage";
import { formatBytes } from "../../utils/units";
import { protoToInstant, relativeTime } from "../../utils/temporal";
import { Badge } from "../Badge";
import { CogIcon } from "../icons";
import { ExpandableCard } from "../settings/ExpandableCard";
import { CrossLinkBadge, CrossLinkChip } from "./CrossLinkBadge";

// storageVerdictLabel renders the storage's badge grammar as text — the
// same two server-computed, hysteresis-aware booleans the admission gate
// itself consults (StorageState.warn_verdict / protect_verdict), never
// re-derived from free/warn/floor here. Matches the CLI's storageVerdict
// (gastrolog-3cobq4 UI/CLI parity) and the vault card's "refusing" grammar:
// a badge appears only while the condition is active, nothing when healthy.
export function storageVerdictLabel(storage: Pick<Storage, "warnVerdict" | "protectVerdict">): "protected" | "warn" | null {
  if (storage.protectVerdict) return "protected";
  if (storage.warnVerdict) return "warn";
  return null;
}

// thresholdLabel renders an effective threshold with its source — the
// resolved bytes value is what matters (placeholder-style, same as the
// Settings form's inherited-value placeholders), with the configured
// expression or "inherited" naming where it came from. Mirrors the CLI's
// thresholdLabel exactly (gastrolog-3cobq4).
export function thresholdLabel(expr: string, inherited: boolean, effectiveBytes: bigint): string {
  const eff = formatBytes(effectiveBytes);
  if (inherited) return `${eff} (inherited)`;
  return `${eff} (${expr})`;
}

interface StorageCardProps {
  storage: Storage;
  dark: boolean;
  expanded: boolean;
  onToggle: () => void;
  onOpenSettings?: () => void;
  /** Switches the inspector to another entity card, e.g. a placed vault. */
  onNavigate?: (param: string) => void;
}

export function StorageCard({
  storage,
  dark,
  expanded,
  onToggle,
  onOpenSettings,
  onNavigate,
}: Readonly<StorageCardProps>) {
  const verdict = storageVerdictLabel(storage);

  return (
    <ExpandableCard
      key={storage.id}
      id={storage.displayLabel}
      typeBadge={`class ${String(storage.storageClass)}`}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          <StorageNodeBadge nodeName={storage.nodeName} dark={dark} />
          {verdict === "protected" && (
            <Badge variant="error" dark={dark}>protected</Badge>
          )}
          {verdict === "warn" && (
            <Badge variant="warn" dark={dark}>warn</Badge>
          )}
          <Badge variant="muted" dark={dark}>
            {formatBytes(storage.freeBytes)} / {formatBytes(storage.totalBytes)}
          </Badge>
          {onOpenSettings && (
            <CrossLinkBadge dark={dark} title="Open in Settings" onClick={onOpenSettings}>
              <CogIcon className="w-3 h-3" />
            </CrossLinkBadge>
          )}
        </span>
      }
    >
      <StorageDetail storage={storage} dark={dark} onNavigate={onNavigate} />
    </ExpandableCard>
  );
}

// StorageNodeBadge mirrors NodeBadge's grammar (this-node copper pill +
// muted node-name pill, hidden in single-node) but keys off the storage's
// published node_name string rather than a node ID — StorageState carries
// no node ID, only the pre-resolved display name (gastrolog-3cobq4).
function StorageNodeBadge({ nodeName, dark }: Readonly<{ nodeName: string; dark: boolean }>) {
  const registry = useNodeRegistry();
  if (!registry.multiNode) return null;
  const node = registry.all.find((n) => n.name === nodeName);
  const isLocal = node ? registry.isLocal(node.id) : false;
  return (
    <>
      {isLocal && <Badge variant="copper" dark={dark}>this node</Badge>}
      <Badge variant="muted" dark={dark}>{nodeName}</Badge>
    </>
  );
}

function StorageDetail({
  storage,
  dark,
  onNavigate,
}: Readonly<{ storage: Storage; dark: boolean; onNavigate?: (param: string) => void }>) {
  const c = useThemeClass(dark);
  const sectionTitle = (title: string) => (
    <h3
      className={`text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
    >
      {title}
    </h3>
  );
  const panelClass = `rounded-lg border px-4 py-3 ${c("border-ink-border bg-ink-well", "border-light-border bg-light-well")}`;
  const labelClass = `text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted", "text-light-text-muted")}`;
  const valueClass = c("text-text-bright", "text-light-text-bright");

  const sampled = storage.sampledAt ? relativeTime(protoToInstant(storage.sampledAt)) : null;

  return (
    <div className="flex flex-col gap-4 pt-2">
      <section className="flex flex-col gap-4">
        {sectionTitle("Identity")}
        <div className={panelClass}>
          <div className="flex flex-wrap items-baseline gap-x-5 gap-y-2 text-[0.85em]">
            <div className="flex items-baseline gap-2">
              <span className={labelClass}>Path</span>
              <span className={`font-mono ${valueClass}`}>{storage.path}</span>
            </div>
          </div>
        </div>
      </section>

      <section className="flex flex-col gap-4">
        {sectionTitle("Thresholds")}
        <div className={panelClass}>
          <div className="flex flex-col gap-2 text-[0.85em]">
            <div className="flex items-baseline gap-2">
              <span className={labelClass}>Warn</span>
              <span className={`font-mono ${valueClass}`}>
                {thresholdLabel(storage.warnExpr, storage.warnInherited, storage.warnBytes)}
              </span>
            </div>
            <div className="flex items-baseline gap-2">
              <span className={labelClass}>Floor</span>
              <span className={`font-mono ${valueClass}`}>
                {thresholdLabel(storage.floorExpr, storage.floorInherited, storage.floorBytes)}
              </span>
            </div>
          </div>
        </div>
      </section>

      <section className="flex flex-col gap-4">
        {sectionTitle("Live State")}
        <div className={panelClass}>
          <div className="flex flex-wrap items-baseline gap-x-5 gap-y-2 text-[0.85em]">
            <div className="flex items-baseline gap-2">
              <span className={labelClass}>Free</span>
              <span className={`font-mono ${valueClass}`}>{formatBytes(storage.freeBytes)}</span>
            </div>
            <div className="flex items-baseline gap-2">
              <span className={labelClass}>Total</span>
              <span className={`font-mono ${valueClass}`}>{formatBytes(storage.totalBytes)}</span>
            </div>
            {sampled && (
              <div className="flex items-baseline gap-2">
                <span className={labelClass}>Sampled</span>
                <span className={`font-mono ${c("text-text-muted", "text-light-text-muted")}`}>{sampled}</span>
              </div>
            )}
            {!storage.hasSample && (
              <span className={c("text-text-muted", "text-light-text-muted")}>No sample yet.</span>
            )}
          </div>
        </div>
      </section>

      <PlacementsSection storage={storage} dark={dark} onNavigate={onNavigate} />
    </div>
  );
}

// PlacementsSection cross-links each placed vault to its own card — the
// config-derived placed_vault_ids field, resolved to display names via the
// same vault list the Vaults tab renders (never re-derived; a raw-ID
// fallback covers a vault this node's config hasn't caught up to). Clicking
// a chip switches the inspector to the Vaults entity list and expands that
// vault's card, the same "entities:<type>:<name>" deep-link format used by
// the settings dialog's cross-links into the inspector.
function PlacementsSection({
  storage,
  dark,
  onNavigate,
}: Readonly<{ storage: Storage; dark: boolean; onNavigate?: (param: string) => void }>) {
  const c = useThemeClass(dark);
  const { data: vaults } = useVaults();
  const vaultById = new Map(vaults.map((v) => [v.id, v]));

  const sectionTitle = (
    <h3
      className={`text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
    >
      Placements
    </h3>
  );
  const panelClass = `rounded-lg border px-4 py-3 ${c("border-ink-border bg-ink-well", "border-light-border bg-light-well")}`;

  return (
    <section className="flex flex-col gap-4">
      {sectionTitle}
      <div className={panelClass}>
        {storage.placedVaultIds.length === 0 ? (
          <span className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
            No vaults placed on this storage.
          </span>
        ) : (
          <div className="flex flex-wrap gap-1.5">
            {storage.placedVaultIds.map((vaultId) => {
              const vault = vaultById.get(vaultId);
              const label = vault?.displayLabel ?? vaultId;
              if (!onNavigate) {
                return (
                  <Badge key={vaultId} variant="muted" dark={dark}>{label}</Badge>
                );
              }
              return (
                <CrossLinkChip
                  key={vaultId}
                  dark={dark}
                  title={`Open vault ${label}`}
                  onClick={() => onNavigate(`entities:vaults:${label}`)}
                >
                  {label}
                </CrossLinkChip>
              );
            })}
          </div>
        )}
      </div>
    </section>
  );
}
