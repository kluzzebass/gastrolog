// Seal-pip state derivation (gastrolog-4fzwu3). Pure functions so the
// grammar is unit-testable apart from rendering. Design reference:
// docs/mockups/seal-pips.html.
//
// Vocabulary: a CHUNK SEAL is the cluster-wide fact (CmdSealChunk applied,
// record membership frozen — happens once). A COPY SEAL is one home's
// completed GLCB build for that chunk. A late copy seal (rejoining node)
// is normal operation; the pip row exists so it reads as catch-up instead
// of "resealing already sealed chunks".

/** Lifecycle state of one pip. Birth fills green; death drains red.
 *  Sealed is deliberately calm: attention belongs to the ANOMALOUS pip,
 *  which renders with a glow in its own color — the eye lands on the node
 *  that has the problem, not on its healthy neighbors. */
export type PipState =
  | "active" // hollow copper ring — chunk open on this node
  | "sealing" // half amber, pulsing — copy seal pending/running here
  | "lagging" // half amber, pulsing + glow — copy pending on an already-sealed chunk (catch-up)
  | "sealed" // calm green — copy sealed (bytes verified on this node)
  | "missing" // dashed red slashed ring + glow — placement node unreachable
  | "holds" // solid red, pulsing — delete requested, node still holds bytes
  | "acked" // dim hollow red — node acked the delete, bytes gone
  | "uncached" // dim hollow green ring — cloud-backed, no local copy on this node (normal)
  | "ghost"; // muted dot after a gap — copy on a node outside placement

export interface SealPip {
  node: string;
  state: PipState;
  title: string;
}

export interface PipInputs {
  /** Cluster-wide chunk lifecycle from the FSM overlay. */
  chunkState: "active" | "sealing" | "sealed";
  /** Placement node names for the vault (expected holders). */
  placementNodes: readonly string[];
  /** Nodes reporting a copy (bytes-truth via holder receipts). */
  residentNodes: readonly string[];
  /** Nodes still owing a delete ack (receipt protocol). */
  pendingAckNodes: readonly string[];
  /** Whether a delete is in flight for this chunk. */
  deleteInFlight: boolean;
  /** Currently reachable node names. */
  liveNodes: ReadonlySet<string>;
}

/** Natural sort: node-2 before node-10. Identical across every row so a
 *  sick node reads as a vertical stripe down the table. */
export function pipOrder(nodes: readonly string[]): string[] {
  return [...nodes].toSorted((a, b) =>
    a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }),
  );
}

/** computePips derives the pip row: placement pips in stable natural-name
 *  order, then ghost pips (residents outside placement) after the gap. */
export function computePips(input: PipInputs): { pips: SealPip[]; ghosts: SealPip[] } {
  const resident = new Set(input.residentNodes);
  const owesAck = new Set(input.pendingAckNodes);
  const placement = pipOrder(input.placementNodes);
  const placementSet = new Set(placement);

  const pips = placement.map((node): SealPip => {
    if (input.deleteInFlight) {
      // Death drains red: the laggard blocking the receipt protocol is
      // the last dot still burning.
      if (owesAck.has(node)) {
        return { node, state: "holds", title: `${node}: delete requested — still holds bytes (owes ack)` };
      }
      return { node, state: "acked", title: `${node}: delete acked — bytes gone` };
    }
    if (!input.liveNodes.has(node)) {
      return { node, state: "missing", title: `${node}: unreachable — copy state unknown` };
    }
    if (resident.has(node)) {
      return { node, state: "sealed", title: `${node}: copy sealed` };
    }
    if (input.chunkState === "active") {
      return { node, state: "active", title: `${node}: chunk active` };
    }
    if (input.chunkState === "sealed") {
      // The chunk seal already happened cluster-wide but this home's copy
      // seal hasn't: the rejoin catch-up shape. Glows so the lagging node
      // is the loudest thing in its row.
      return { node, state: "lagging", title: `${node}: copy lagging — chunk is sealed, this node's copy is still building or queued` };
    }
    return { node, state: "sealing", title: `${node}: copy seal pending — building or queued` };
  });

  const ghosts = ghostPips(input.residentNodes, placementSet);

  return { pips, ghosts };
}

/** computeCloudPips derives the pip row for a cloud-backed chunk: bytes are
 *  durable in the blob store, so pips report LOCAL CACHE state per placement
 *  node — calm green for a cached copy, a dim hollow green ring where no
 *  local copy exists (normal after eviction), dashed red for an unreachable
 *  node. Same order and column stability as the birth/death row. */
export function computeCloudPips(
  input: Pick<PipInputs, "placementNodes" | "residentNodes" | "liveNodes">,
  storeLabel: string,
): { pips: SealPip[]; ghosts: SealPip[] } {
  const resident = new Set(input.residentNodes);
  const placement = pipOrder(input.placementNodes);
  const placementSet = new Set(placement);

  const pips = placement.map((node): SealPip => {
    if (!input.liveNodes.has(node)) {
      return { node, state: "missing", title: `${node}: unreachable — local cache state unknown` };
    }
    if (resident.has(node)) {
      return { node, state: "sealed", title: `${node}: local copy cached — bytes also durable in ${storeLabel}` };
    }
    return { node, state: "uncached", title: `${node}: no local copy — bytes served from ${storeLabel}` };
  });

  return { pips, ghosts: ghostPips(input.residentNodes, placementSet) };
}

function ghostPips(residentNodes: readonly string[], placementSet: ReadonlySet<string>): SealPip[] {
  return pipOrder(residentNodes.filter((n) => !placementSet.has(n))).map(
    (node): SealPip => ({
      node,
      state: "ghost",
      title: `${node}: stale residency — copy present, node not in placement`,
    }),
  );
}
