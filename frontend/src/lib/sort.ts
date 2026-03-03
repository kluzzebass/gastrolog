export function sortByName<T extends { name: string }>(items: T[]): T[] {
  return items.toSorted((a, b) => a.name.localeCompare(b.name));
}
