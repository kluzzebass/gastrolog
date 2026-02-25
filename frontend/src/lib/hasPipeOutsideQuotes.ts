/** Check if the input contains a `|` outside of quoted strings and comments. */
export function hasPipeOutsideQuotes(input: string): boolean {
  let inQuote: string | null = null;
  for (let i = 0; i < input.length; i++) {
    const ch = input[i]!;
    if (inQuote) {
      if (ch === "\\" && i + 1 < input.length) {
        i++; // skip escaped char
      } else if (ch === inQuote) {
        inQuote = null;
      }
    } else if (ch === '"' || ch === "'") {
      inQuote = ch;
    } else if (ch === "#") {
      // Skip to end of line (comment).
      while (i < input.length && input[i] !== "\n") i++;
    } else if (ch === "|") {
      return true;
    }
  }
  return false;
}
