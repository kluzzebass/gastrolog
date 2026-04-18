/**
 * Per-peer inter-node gRPC tx/rx for a single node. Reads cumulative counters
 * from NodeStats.peerBytes and derives rates by differencing consecutive
 * samples in a small rolling window. Covers ALL cluster transport traffic —
 * Raft, broadcast, tier replication, query forwarding, chunk streaming,
 * drain, etc. See gastrolog-47u85.
 */
import { useState } from "react";
import { useThemeClass } from "../../hooks/useThemeClass";
import { formatBytes } from "../../utils";
import type { NodeStats, PeerBytesStat } from "../../api/gen/gastrolog/v1/cluster_pb";

// Rolling window of rate samples, kept small to keep the sparkline readable
// and memory bounded. Broadcast interval is ~5s, so 20 samples ≈ 100s of
// history — enough to eyeball a recent-trend sparkline without hoarding.
const SPARK_POINTS = 20;

interface SampleWindow {
  // Cumulative counters from the most recent update, used to compute the
  // next rate by differencing against the next sample.
  lastSent: number;
  lastRecv: number;
  lastAt: number;
  // Rolling rates (bytes/sec) for the sparkline.
  txRates: number[];
  rxRates: number[];
}

// peerKey returns the peer identifier used as the state-map key. Uses the
// proto-defined peer string (node ID) directly.
function peerKey(p: PeerBytesStat): string {
  return p.peer;
}

export interface PeerBytesSectionProps {
  readonly nodeStats: NodeStats | null | undefined;
  readonly peerNameById: ReadonlyMap<string, string>;
  readonly dark: boolean;
}

type PeerRate = { tx: number; rx: number; txSpark: number[]; rxSpark: number[] };

type WindowState = {
  windows: Map<string, SampleWindow>;
  rates: Map<string, PeerRate>;
};

function emptyWindowState(): WindowState {
  return { windows: new Map(), rates: new Map() };
}

export function PeerBytesSection({ nodeStats, peerNameById, dark }: PeerBytesSectionProps) {
  const c = useThemeClass(dark);

  // Sample windows and derived rates are held together in one state slot
  // so they advance transactionally whenever a fresh proto payload arrives.
  // No useEffect, no refs — React Compiler memoizes the whole thing.
  const [state, setState] = useState<WindowState>(emptyWindowState);
  const [lastInput, setLastInput] = useState<readonly PeerBytesStat[] | null>(null);

  const peerBytes: readonly PeerBytesStat[] = nodeStats?.peerBytes ?? [];

  // Proto objects are re-created on every update; an identity mismatch
  // against the last observed input means we have a fresh sample. Commit
  // the new state during render — React re-runs with the updated state
  // rather than scheduling a follow-up effect pass.
  if (peerBytes !== lastInput) {
    setLastInput(peerBytes);
    setState((prev) => stepWindows(peerBytes, prev));
  }
  const rates = state.rates;

  if (peerBytes.length === 0) {
    return (
      <div className={`text-[0.85em] ${c("text-text-muted", "text-light-text-muted")}`}>
        No inter-node traffic recorded for this node.
      </div>
    );
  }

  const rows = [...peerBytes].sort((a, b) => {
    const na = peerNameById.get(a.peer) ?? a.peer;
    const nb = peerNameById.get(b.peer) ?? b.peer;
    return na.localeCompare(nb);
  });

  return (
    <div
      className={`rounded-md border overflow-hidden ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
    >
      <table className="w-full text-[0.8em] font-mono">
        <thead>
          <tr className={c("bg-ink-surface/80", "bg-light-surface/80")}>
            <th className={`px-3 py-1.5 text-left font-medium ${c("text-text-muted", "text-light-text-muted")}`}>Peer</th>
            <th className={`px-3 py-1.5 text-right font-medium ${c("text-text-muted", "text-light-text-muted")}`}>Tx/s</th>
            <th className={`px-3 py-1.5 w-16 ${c("text-text-muted", "text-light-text-muted")}`}></th>
            <th className={`px-3 py-1.5 text-right font-medium ${c("text-text-muted", "text-light-text-muted")}`}>Rx/s</th>
            <th className={`px-3 py-1.5 w-16 ${c("text-text-muted", "text-light-text-muted")}`}></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((p) => {
            const k = peerKey(p);
            const r = rates.get(k);
            const name = peerNameById.get(p.peer) ?? p.peer;
            return (
              <tr
                key={k}
                className={`border-t ${c("border-ink-border-subtle", "border-light-border-subtle")}`}
              >
                <td className={`px-3 py-1.5 truncate ${c("text-text-bright", "text-light-text-bright")}`}>{name}</td>
                <td className={`px-3 py-1.5 text-right ${c("text-text-bright", "text-light-text-bright")}`}>
                  {r ? `${formatBytes(Math.round(r.tx))}/s` : "—"}
                </td>
                <td className="px-3 py-1 text-copper">
                  <Spark values={r?.txSpark ?? []} />
                </td>
                <td className={`px-3 py-1.5 text-right ${c("text-text-bright", "text-light-text-bright")}`}>
                  {r ? `${formatBytes(Math.round(r.rx))}/s` : "—"}
                </td>
                <td className="px-3 py-1 text-copper-dim">
                  <Spark values={r?.rxSpark ?? []} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// stepWindows folds a fresh peerBytes sample into the rolling-window state
// and returns a new WindowState with both maps rebuilt. Pure function —
// the previous state is never mutated, so React's state-identity checks
// work and the ring buffer history survives cleanly across renders.
function stepWindows(peerBytes: readonly PeerBytesStat[], prev: WindowState): WindowState {
  const now = performance.now();
  const windows = new Map<string, SampleWindow>();
  const rates = new Map<string, PeerRate>();

  for (const p of peerBytes) {
    const k = peerKey(p);
    const sent = Number(p.bytesSent);
    const recv = Number(p.bytesReceived);
    const w = prev.windows.get(k);
    if (!w) {
      windows.set(k, {
        lastSent: sent,
        lastRecv: recv,
        lastAt: now,
        txRates: [],
        rxRates: [],
      });
      rates.set(k, { tx: 0, rx: 0, txSpark: [], rxSpark: [] });
      continue;
    }
    const dt = (now - w.lastAt) / 1000;
    if (dt <= 0) {
      // Clock went backwards or two samples at the same instant —
      // preserve existing state, no new rate.
      windows.set(k, w);
      rates.set(k, { tx: 0, rx: 0, txSpark: [...w.txRates], rxSpark: [...w.rxRates] });
      continue;
    }
    const tx = Math.max(0, (sent - w.lastSent) / dt);
    const rx = Math.max(0, (recv - w.lastRecv) / dt);
    const txRates = [...w.txRates, tx];
    const rxRates = [...w.rxRates, rx];
    if (txRates.length > SPARK_POINTS) txRates.shift();
    if (rxRates.length > SPARK_POINTS) rxRates.shift();
    windows.set(k, {
      lastSent: sent,
      lastRecv: recv,
      lastAt: now,
      txRates,
      rxRates,
    });
    rates.set(k, { tx, rx, txSpark: txRates, rxSpark: rxRates });
  }

  return { windows, rates };
}

// Spark renders a minimal inline line chart. Fixed-size SVG; auto-scales
// Y-axis to the window's own max so quiet and busy peers each fill the
// band meaningfully. Color comes from the parent via currentColor —
// callers set a Tailwind text-* class on the wrapper to pick the hue.
function Spark({ values }: Readonly<{ values: number[] }>) {
  if (values.length < 2) {
    return <svg width="56" height="16" aria-hidden="true" />;
  }
  const w = 56;
  const h = 16;
  const max = Math.max(...values, 1);
  const step = w / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = i * step;
      const y = h - (v / max) * h;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg width={w} height={h} aria-hidden="true">
      <polyline fill="none" stroke="currentColor" strokeWidth="1.5" points={points} />
    </svg>
  );
}
