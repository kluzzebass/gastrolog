/** Returns a helper that picks dark or light class strings based on the current theme. */
export function useThemeClass(dark: boolean): (d: string, l: string) => string {
  return dark ? (d: string) => d : (_: string, l: string) => l;
}
