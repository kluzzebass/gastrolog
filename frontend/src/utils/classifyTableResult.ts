export type TableClassification =
  | "single-value"
  | "bar-chart"
  | "donut-chart"
  | "world-map"
  | "scatter-map"
  | "table";

/** True if the column name looks like a country code field (e.g. "src_ip_country", "country"). */
function isCountryColumn(name: string): boolean {
  const lower = name.toLowerCase();
  return lower === "country" || lower.endsWith("_country");
}

/** True if the value looks like an ISO 3166-1 alpha-2 country code. */
function looksLikeIsoCode(val: string): boolean {
  return val.length === 2 && /^[A-Z]{2}$/.test(val);
}

function isLatColumn(name: string): boolean {
  const lower = name.toLowerCase();
  return lower === "latitude" || lower.endsWith("_latitude");
}

function isLonColumn(name: string): boolean {
  const lower = name.toLowerCase();
  return lower === "longitude" || lower.endsWith("_longitude");
}

/** Find indices of latitude and longitude columns. Returns [latIdx, lonIdx] or null. */
export function findLatLonColumns(columns: string[]): [number, number] | null {
  let latIdx = -1;
  let lonIdx = -1;
  for (let i = 0; i < columns.length; i++) {
    if (latIdx === -1 && isLatColumn(columns[i]!)) latIdx = i;
    else if (lonIdx === -1 && isLonColumn(columns[i]!)) lonIdx = i;
  }
  if (latIdx === -1 || lonIdx === -1) return null;
  return [latIdx, lonIdx];
}

/**
 * Classifies a table result to determine the best visualization.
 *
 * Rules:
 * - 1 column, 1 row → "single-value"
 * - < 2 columns or < 2 rows → "table"
 * - Last column not all-numeric → "table"
 * - Country column + numeric aggregate → "world-map"
 * - Lat/lon columns + numeric aggregate → "scatter-map"
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

  // Check if the last column is all-numeric.
  const lastColIdx = columns.length - 1;
  const allNumeric = rows.every((row) => {
    const val = row[lastColIdx];
    if (val === undefined || val === "") return false;
    return !isNaN(Number(val));
  });
  if (!allNumeric) return "table";

  // World map: 2 columns, first is a country column where most values are ISO alpha-2 codes.
  // Empty values are allowed (unresolved IPs produce blank country fields).
  if (
    columns.length === 2 &&
    rows.length >= 2 &&
    isCountryColumn(columns[0]!) &&
    rows.every((row) => {
      const v = row[0] ?? "";
      return v === "" || looksLikeIsoCode(v);
    })
  ) {
    return "world-map";
  }

  // Scatter map: lat + lon columns with a numeric aggregate.
  // Needs at least 3 columns (lat, lon, value) and 2+ rows.
  if (columns.length >= 3 && rows.length >= 2 && findLatLonColumns(columns)) {
    return "scatter-map";
  }

  if (rows.length > 20) return "table";

  if (columns.length === 2 && rows.length >= 2 && rows.length <= 6) {
    return "donut-chart";
  }

  return "bar-chart";
}
