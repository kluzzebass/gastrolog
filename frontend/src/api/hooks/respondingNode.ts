import { useSyncExternalStore } from "react";
import { type EntityID, EMPTY_ID } from "../model/id";

/**
 * Which node answers this browser's RPCs.
 *
 * The API has no session affinity: behind a load balancer (the k8s NodePort
 * is one) every request may land on a different node, and nothing in a
 * response lets the UI pick a node to stick to. The only honest thing the UI
 * can know is whether the answering node has varied so far. Each response
 * that names its responding node is recorded here; once two distinct nodes
 * have answered, nothing in this session may call any node "this node".
 */
const observed = new Set<EntityID>();
const listeners = new Set<() => void>();

function notify() {
  for (const l of listeners) l();
}

/** Record the node that produced a response. Empty IDs carry no information. */
export function recordRespondingNode(id: EntityID): void {
  if (id === EMPTY_ID || observed.has(id)) return;
  observed.add(id);
  notify();
}

/** True until two distinct nodes have answered this session's RPCs. */
export function respondingNodeIsStable(): boolean {
  return observed.size <= 1;
}

/** Test seam: forget every observation. */
export function resetRespondingNodeObservation(): void {
  observed.clear();
  notify();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useRespondingNodeStable(): boolean {
  return useSyncExternalStore(subscribe, respondingNodeIsStable, respondingNodeIsStable);
}
