import { findTopic } from "../../help/topics";

/** Every ingester type has a help topic named after it. */
export function ingesterHelpRef(type: string): string {
  return `ingester-${type}`;
}

/** The operator-facing name of an ingester type: its help topic's title, so the menu, the badges and the help agree. */
export function ingesterTypeLabel(type: string): string {
  return findTopic(ingesterHelpRef(type))?.title ?? type;
}
