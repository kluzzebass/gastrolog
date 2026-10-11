import { useThemeClass } from "../../hooks/useThemeClass";
import { useVaults } from "../../api/hooks";
import type { Storage } from "../../api/model/storage";
import { BYTES_MAX_CHARS, formatBytes } from "../../utils/units";
import { LiveValue } from "../LiveValue";
import { protoToInstant, relativeTime } from "../../utils/temporal";
import { Badge } from "../Badge";
import { HelpButton } from "../HelpButton";
import { CogIcon } from "../icons";
import { ExpandableCard } from "../settings/ExpandableCard";
import { NodeBadge } from "../settings/NodeBadge";
import { CrossLinkBadge, CrossLinkChip } from "./CrossLinkBadge";
import { freeOfTotalLabel, storageVerdictLabel, thresholdLabel } from "./storageLabels";

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
      typeBadge={{
        text: `class ${String(storage.storageClass)}`,
        title: "storage class — lower is faster",
        helpRef: "storage-config#file-storage",
      }}
      dark={dark}
      expanded={expanded}
      onToggle={onToggle}
      headerRight={
        <span className="flex items-center gap-1.5">
          <NodeBadge nodeId={storage.nodeId} dark={dark} />
          {verdict === "protected" && (
            <Badge
              variant="error"
              dark={dark}
              title="below the disk-free floor — every vault here refuses new records"
              helpRef="inspector-storages#storage-overview"
            >
              protected
            </Badge>
          )}
          {verdict === "warn" && (
            <Badge
              variant="warn"
              dark={dark}
              title="below the disk-free warn threshold — the disk-space alarm stands; ingestion continues"
              helpRef="inspector-storages#storage-overview"
            >
              warn
            </Badge>
          )}
          <Badge
            variant="muted"
            dark={dark}
            title={`${freeOfTotalLabel(storage.freeBytes, storage.totalBytes)} at the last disk-guard sample`}
          >
            <LiveValue dark={dark} tone="inherit" reserve={BYTES_MAX_CHARS}>
              {formatBytes(storage.freeBytes)}
            </LiveValue>
            {" free of "}
            {formatBytes(storage.totalBytes)}
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

function StorageDetail({
  storage,
  dark,
  onNavigate,
}: Readonly<{ storage: Storage; dark: boolean; onNavigate?: (param: string) => void }>) {
  const c = useThemeClass(dark);
  const sectionTitle = (title: string, helpRef?: string) => (
    <h3
      className={`flex items-center gap-1.5 text-[0.75em] font-medium uppercase tracking-[0.15em] whitespace-nowrap ${c("text-text-muted", "text-light-text-muted")}`}
    >
      {title}
      {helpRef && <HelpButton helpRef={helpRef} />}
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
        {sectionTitle("Thresholds", "storage-config#file-storage")}
        <div className={panelClass}>
          <div className="flex flex-col gap-2 text-[0.85em]">
            <div className="flex items-baseline gap-2">
              <span className={labelClass}>Warn</span>
              <span className={`font-mono ${valueClass}`}>
                {thresholdLabel(storage.warnExpr, storage.warnIsDefault, storage.warnBytes)}
              </span>
            </div>
            <div className="flex items-baseline gap-2">
              <span className={labelClass}>Floor</span>
              <span className={`font-mono ${valueClass}`}>
                {thresholdLabel(storage.floorExpr, storage.floorIsDefault, storage.floorBytes)}
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
              <LiveValue dark={dark} reserve={BYTES_MAX_CHARS}>
                {formatBytes(storage.freeBytes)}
              </LiveValue>
            </div>
            <div className="flex items-baseline gap-2">
              <span className={labelClass}>Total</span>
              <LiveValue dark={dark} align="start">
                {formatBytes(storage.totalBytes)}
              </LiveValue>
            </div>
            {sampled && (
              <div className="flex items-baseline gap-2">
                <span className={labelClass}>Sampled</span>
                <LiveValue dark={dark} tone="muted" align="start">
                  {sampled}
                </LiveValue>
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
