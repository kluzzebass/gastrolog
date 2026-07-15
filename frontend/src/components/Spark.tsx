/**
 * Spark renders a tiny inline polyline of recent samples (rolling-window rate
 * history). Scaled to its own max; renders an empty box below two samples so
 * layouts stay stable while a series warms up.
 */
export function Spark({
  values,
  width = 56,
  height = 16,
}: Readonly<{ values: readonly number[]; width?: number; height?: number }>) {
  if (values.length < 2) {
    return <svg width={width} height={height} aria-hidden="true" />;
  }
  const max = Math.max(...values, 1);
  const step = width / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = i * step;
      const y = height - (v / max) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg width={width} height={height} aria-hidden="true">
      <polyline
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        points={points}
      />
    </svg>
  );
}
