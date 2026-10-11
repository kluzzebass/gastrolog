import type { EntityType } from "./InspectorDialog";

const entityCountNouns: Record<EntityType, [singular: string, plural: string]> = {
  vaults: ["vault configured", "vaults configured"],
  storages: ["file storage", "file storages"],
  ingesters: ["ingester configured", "ingesters configured"],
  routes: ["route configured", "routes configured"],
  jobs: ["job active or scheduled", "jobs active or scheduled"],
  system: ["live node", "live nodes"],
};

/** The hover text of a nav count: the number with the noun it counts. */
export function entityCountTitle(entityType: EntityType, count: number): string {
  const [one, many] = entityCountNouns[entityType];
  return `${count} ${count === 1 ? one : many}`;
}

/** The per-node header count on the Jobs tab, with its noun. */
export function jobCountText(n: number): string {
  return `${n} ${n === 1 ? "job" : "jobs"}`;
}
