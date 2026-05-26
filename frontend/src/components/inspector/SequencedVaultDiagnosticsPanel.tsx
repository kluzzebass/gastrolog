import { useThemeClass } from "../../hooks/useThemeClass";
import { useSequencedVaultDiagnostics } from "../../api/hooks";
import { useClusterStatus } from "../../api/hooks/useClusterStatus";
import { useConfig } from "../../api/hooks/useSystem";
import { buildNodeNameMap } from "../../utils/nodeNames";
import { Badge } from "../Badge";
import {
  allocatorSummary,
  extractPeerSequencedWatermarks,
  formatFenceCreatedAt,
  formatSeq,
  peerSpoolDivergence,
  sequencedLagWarnings,
  watermarksFromDiagnostics,
} from "../../utils/sequencedDiagnostics";

interface SequencedVaultDiagnosticsPanelProps {
  vaultId: string;
  dark: boolean;
}

export function SequencedVaultDiagnosticsPanel({
  vaultId,
  dark,
}: Readonly<SequencedVaultDiagnosticsPanelProps>) {
  const c = useThemeClass(dark);
  const { data: cluster } = useClusterStatus();
  const { data: config } = useConfig();
  const { data: diag, isLoading, isError, error } = useSequencedVaultDiagnostics(vaultId, true);

  const nodeNames = buildNodeNameMap(config?.nodeConfigs ?? []);
  const peers = extractPeerSequencedWatermarks(
    vaultId,
    cluster?.nodes ?? [],
    nodeNames,
  );
  const peerDivergence = peerSpoolDivergence(peers);

  const localWarnings = diag ? sequencedLagWarnings(watermarksFromDiagnostics(diag)) : [];
  const alloc = allocatorSummary(diag?.allocator);

  return (
    <div
      className={`px-4 py-3 border-b ${c(
        "border-ink-border-subtle bg-ink-raised/40",
        "border-light-border-subtle bg-light-bg/40",
      )}`}
    >
      <div className="flex items-center gap-2 mb-3">
        <SectionLabel dark={dark}>Sequenced diagnostics</SectionLabel>
        <Badge variant="info" dark={dark}>sequenced</Badge>
      </div>

      {(localWarnings.length > 0 || peerDivergence) && (
        <div className="flex flex-col gap-1.5 mb-3">
          {localWarnings.map((msg) => (
            <LagBanner key={msg} message={msg} variant="warn" dark={dark} />
          ))}
          {peerDivergence && <LagBanner message={peerDivergence} variant="error" dark={dark} />}
        </div>
      )}

      <Subsection title="Local node" dark={dark}>
        {isLoading && (
          <MutedText dark={dark}>Loading local diagnostics...</MutedText>
        )}
        {!isLoading && isError && (
          <MutedText dark={dark}>
            {error instanceof Error ? error.message : "Local diagnostics unavailable on this node."}
          </MutedText>
        )}
        {!isLoading && !isError && diag && (
          <>
            <WatermarkGrid
              dark={dark}
              rows={[
                ["S_r (spool)", formatSeq(diag.spoolWatermark)],
                ["H (ingest)", formatSeq(diag.ingestHighWatermark)],
                ["F_n (fence)", formatSeq(diag.fenceHighWatermark)],
                ["M_r (materialized)", formatSeq(diag.materializationWatermark)],
                ["C_r (converged)", formatSeq(diag.convergenceWatermark)],
              ]}
            />
            {diag.nodeId && (
              <div className="mt-2">
                <CompactStat label="Node" value={diag.nodeId} mono dark={dark} />
              </div>
            )}
          </>
        )}
      </Subsection>

      {!isLoading && !isError && diag && (
        <>
          <Subsection title="Allocator" dark={dark}>
            <WatermarkGrid
              dark={dark}
              rows={[
                ["Next seq", alloc.nextSeq],
                ["Epoch", alloc.epoch],
              ]}
            />
            {alloc.activeSwaths.length === 0 ? (
              <MutedText dark={dark}>Active swaths: none</MutedText>
            ) : (
              <div className="mt-2 flex flex-col gap-1">
                <MutedText dark={dark}>Active swaths</MutedText>
                {alloc.activeSwaths.map((sw) => (
                  <div
                    key={`${sw.holderId}-${formatSeq(sw.rangeStart)}`}
                    className={`font-mono text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
                  >
                    {`holder=${sw.holderId} epoch=${formatSeq(sw.epoch)} range=${formatSeq(sw.rangeStart)}-${formatSeq(sw.rangeEnd)}`}
                  </div>
                ))}
              </div>
            )}
            {alloc.burnedTails.length === 0 ? (
              <MutedText dark={dark}>Burned tails: none</MutedText>
            ) : (
              <div className="mt-2 flex flex-col gap-1">
                <MutedText dark={dark}>Burned tails</MutedText>
                {alloc.burnedTails.map((tail) => (
                  <div
                    key={`${formatSeq(tail.start)}-${formatSeq(tail.end)}`}
                    className={`font-mono text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
                  >
                    {`${formatSeq(tail.start)}-${formatSeq(tail.end)} (epoch ${formatSeq(tail.epoch)})`}
                  </div>
                ))}
              </div>
            )}
          </Subsection>

          <Subsection title="Fences" dark={dark}>
            {diag.fences.length === 0 ? (
              <MutedText dark={dark}>No fences published</MutedText>
            ) : (
              <div className="flex flex-col gap-1">
                {diag.fences.map((fence) => (
                  <div
                    key={formatSeq(fence.id)}
                    className={`font-mono text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
                  >
                    {`F_${formatSeq(fence.id)} upper=${formatSeq(fence.upperBoundSeq)} prev=${formatSeq(fence.prevBoundSeq)} created=${formatFenceCreatedAt(fence)}`}
                  </div>
                ))}
              </div>
            )}
          </Subsection>
        </>
      )}

      {peers.length > 0 && (
        <Subsection title="Cluster replica watermarks" dark={dark}>
          <div className="flex flex-col gap-1">
            {peers.map((peer) => (
              <div
                key={peer.nodeId}
                className={`font-mono text-[0.8em] ${c("text-text-muted", "text-light-text-muted")}`}
              >
                {`${peer.nodeName}: H=${formatSeq(peer.watermarks.h)} S_r=${formatSeq(peer.watermarks.sR)} F_n=${formatSeq(peer.watermarks.fN)} M_r=${formatSeq(peer.watermarks.mR)} C_r=${formatSeq(peer.watermarks.cR)}`}
              </div>
            ))}
          </div>
        </Subsection>
      )}
    </div>
  );
}

/** Compact watermark grid for node stats (SystemStatsView). */
export function SequencedVaultWatermarksTable({
  vaultName,
  vaultId,
  peers,
  dark,
}: Readonly<{
  vaultName: string;
  vaultId: string;
  peers: ReturnType<typeof extractPeerSequencedWatermarks>;
  dark: boolean;
}>) {
  const c = useThemeClass(dark);
  const divergence = peerSpoolDivergence(peers);

  return (
    <div className="flex flex-col gap-1.5">
      <div className={`text-[0.8em] font-medium ${c("text-text-bright", "text-light-text-bright")}`}>
        {`${vaultName} (${vaultId.slice(0, 8)}…)`}
      </div>
      {divergence && <LagBanner message={divergence} variant="error" dark={dark} />}
      {peers.map((peer) => (
        <div
          key={peer.nodeId}
          className={`font-mono text-[0.75em] pl-2 ${c("text-text-muted", "text-light-text-muted")}`}
        >
          {`${peer.nodeName}: H=${formatSeq(peer.watermarks.h)} S_r=${formatSeq(peer.watermarks.sR)} F_n=${formatSeq(peer.watermarks.fN)} M_r=${formatSeq(peer.watermarks.mR)} C_r=${formatSeq(peer.watermarks.cR)}`}
        </div>
      ))}
    </div>
  );
}

function SectionLabel({ dark, children }: Readonly<{ dark: boolean; children: React.ReactNode }>) {
  const c = useThemeClass(dark);
  return (
    <span
      className={`text-[0.7em] font-medium uppercase tracking-[0.15em] ${c("text-text-muted", "text-light-text-muted")}`}
    >
      {children}
    </span>
  );
}

function Subsection({
  title,
  dark,
  children,
}: Readonly<{ title: string; dark: boolean; children: React.ReactNode }>) {
  const c = useThemeClass(dark);
  return (
    <div className="mt-3 first:mt-0">
      <div
        className={`text-[0.7em] font-medium uppercase tracking-[0.12em] mb-1.5 ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {title}
      </div>
      {children}
    </div>
  );
}

function WatermarkGrid({
  dark,
  rows,
}: Readonly<{ dark: boolean; rows: [string, string][] }>) {
  return (
    <div className="grid grid-cols-2 gap-x-6 gap-y-1">
      {rows.map(([label, value]) => (
        <CompactStat key={label} label={label} value={value} mono dark={dark} />
      ))}
    </div>
  );
}

function CompactStat({
  label,
  value,
  mono = false,
  dark,
}: Readonly<{ label: string; value: string; mono?: boolean; dark: boolean }>) {
  const c = useThemeClass(dark);
  return (
    <div className="flex items-baseline justify-between gap-4">
      <span className={`text-[0.75em] shrink-0 ${c("text-text-muted", "text-light-text-muted")}`}>
        {label}
      </span>
      <span
        className={`text-[0.8em] text-right ${mono ? "font-mono" : ""} ${c("text-text-muted", "text-light-text-muted")}`}
      >
        {value}
      </span>
    </div>
  );
}

function MutedText({ dark, children }: Readonly<{ dark: boolean; children: React.ReactNode }>) {
  const c = useThemeClass(dark);
  return (
    <div className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
      {children}
    </div>
  );
}

function LagBanner({
  message,
  variant,
}: Readonly<{ message: string; variant: "warn" | "error"; dark: boolean }>) {
  const color = variant === "error" ? "text-severity-error" : "text-severity-warn";
  return (
    <div className={`text-[0.8em] ${color}`}>
      {message}
    </div>
  );
}
