/** Responsive middle-truncation for monospace IDs.
 *
 * Splits the text into a head (flexible, truncates with CSS ellipsis) and
 * a tail (fixed, never shrinks). As the container narrows, the CSS ellipsis
 * eats into the middle — preserving both the timestamp prefix and the
 * random suffix of UUIDv7/GLID strings.
 *
 * Full text is in the title attribute for hover/copy.
 */
export function MiddleTruncate({
  text,
  tailChars = 8,
  className = "",
}: {
  text: string;
  tailChars?: number;
  className?: string;
}) {
  if (!text) return null;
  const split = Math.max(0, text.length - tailChars);
  const head = text.slice(0, split);
  const tail = text.slice(split);

  return (
    <span className={`flex overflow-hidden ${className}`} title={text}>
      <span className="overflow-hidden text-ellipsis whitespace-nowrap">{head}</span>
      <span className="shrink-0">{tail}</span>
    </span>
  );
}
