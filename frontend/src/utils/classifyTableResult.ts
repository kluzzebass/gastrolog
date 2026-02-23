export type TableClassification = "single-value" | "bar-chart" | "donut-chart" | "table";

/**
 * Classifies a table result to determine the best visualization.
 *
 * Rules:
 * - 1 column, 1 row → "single-value"
 * - < 2 columns or < 2 rows → "table"
 * - Last column not all-numeric → "table"
 * - 2 columns, 2-6 rows → "donut-chart"
 * - 2+ columns, 2-20 rows, numeric aggregate → "bar-chart"
 * - > 20 rows → "table"
 */
export function classifyTableResult(
  columns: string[],
  rows: string[][],
): TableClassification {
  if (columns.length === 1 && rows.length === 1) return "single-value";
  if (columns.length < 2 || rows.length < 2) return "table";
  if (rows.length > 20) return "table";

  // Check if the last column is all-numeric.
  const lastColIdx = columns.length - 1;
  const allNumeric = rows.every((row) => {
    const val = row[lastColIdx];
    if (val === undefined || val === "") return false;
    return !isNaN(Number(val));
  });
  if (!allNumeric) return "table";

  if (columns.length === 2 && rows.length >= 2 && rows.length <= 6) {
    return "donut-chart";
  }

  return "bar-chart";
}
