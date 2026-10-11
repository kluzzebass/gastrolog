/** Why this node cannot join another cluster, or undefined when it can. */
export function joinIneligibility(memberCount: number): string | undefined {
  if (memberCount <= 1) return undefined;
  return `Only a single-node cluster can join another — this node is already one of ${String(memberCount)} members.`;
}
