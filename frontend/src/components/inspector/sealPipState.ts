// Seal-pip state derivation (gastrolog-4fzwu3). Pure functions so the
// grammar is unit-testable apart from rendering. Design reference:
// docs/mockups/seal-pips.html.
//
// Vocabulary: a CHUNK SEAL is the cluster-wide fact (CmdSealChunk applied,
// record membership frozen — happens once). A COPY SEAL is one home's
// completed GLCB build for that chunk. A late copy seal (rejoining node)
// is normal operation; the pip row exists so it reads as catch-up instead
// of "resealing already sealed chunks".

/** Lifecycle state of one pip. Birth fills green; death drains red. */
export type PipState =
  | "active" // hollow copper ring — chunk open on this node
  | "sealing" // half amber, pulsing — copy seal pending/running here
  | "sealed" // solid green — copy sealed (bytes verified on this node)
  | "sealedCalm" // dim green — sealed AND the whole row is healthy
  | "missing" // dashed red slashed ring — placement node unreachable
  | "holds" // solid red, pulsing — delete requested, node still holds bytes
  | "acked" // dim hollow red — node acked the delete, bytes gone
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

  const rowHealthy =
    !input.deleteInFlight &&
    input.chunkState === "sealed" &&
    placement.length > 0 &&
    placement.every((n) => resident.has(n) && input.liveNodes.has(n)) &&
    input.residentNodes.every((n) => placementSet.has(n));

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
      if (rowHealthy) {
        return { node, state: "sealedCalm", title: `${node}: copy sealed — all copies healthy` };
      }
      // Bright green is deliberate: this copy is fine, but the ROW diverges
      // (a neighbor's copy is pending, a node is unreachable, or a ghost
      // exists) — brightness marks rows worth a look.
      return {
        node,
        state: "sealed",
        title: `${node}: copy sealed — highlighted because other copies in this row are pending, missing, or stale`,
      };
    }
    if (input.chunkState === "active") {
      return { node, state: "active", title: `${node}: chunk active` };
    }
    // Chunk seal exists (or is landing) cluster-wide but this home's copy
    // seal hasn't: building or queued — the rejoin catch-up shape.
    return { node, state: "sealing", title: `${node}: copy seal pending — building or queued` };
  });

  const ghosts = pipOrder(input.residentNodes.filter((n) => !placementSet.has(n))).map(
    (node): SealPip => ({
      node,
      state: "ghost",
      title: `${node}: stale residency — copy present, node not in placement`,
    }),
  );

  return { pips, ghosts };
}
