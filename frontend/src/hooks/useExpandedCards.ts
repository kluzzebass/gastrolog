import { useState } from "react";

/** Single-expand: at most one card open at a time. */
export function useExpandedCard(initial: string | null = null) {
  const [expanded, setExpanded] = useState<string | null>(initial);
  const toggle = (id: string) => setExpanded((prev) => (prev === id ? null : id));
  const isExpanded = (id: string) => expanded === id;
  return { expanded, setExpanded, toggle, isExpanded } as const;
}

/** Multi-expand: any number of cards open simultaneously. */
export function useExpandedCards(initial: Record<string, boolean> = {}) {
  const [expandedCards, setExpandedCards] = useState<Record<string, boolean>>(initial);
  const toggle = (key: string) =>
    setExpandedCards((prev) => ({ ...prev, [key]: !prev[key] }));
  const isExpanded = (key: string) => !!expandedCards[key];
  return { expandedCards, setExpandedCards, toggle, isExpanded } as const;
}
