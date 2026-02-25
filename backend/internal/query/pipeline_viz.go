package query

import (
	"strconv"

	"gastrolog/internal/querylang"
)

// ValidateVizOp checks whether the given visualization operator is compatible
// with the table data. Returns the result_type string on success, or empty
// string if validation fails (caller should fall back to "table").
func ValidateVizOp(op querylang.PipeOp, table *TableResult) string {
	switch v := op.(type) {
	case *querylang.BarchartOp:
		if validateBarchart(table) {
			return "barchart"
		}
	case *querylang.DonutOp:
		if validateDonut(table) {
			return "donut"
		}
	case *querylang.MapOp:
		return validateMap(v, table)
	}
	return ""
}

// validateBarchart checks: ≥2 columns, ≥2 rows, last column parseable as float.
func validateBarchart(table *TableResult) bool {
	if len(table.Columns) < 2 || len(table.Rows) < 2 {
		return false
	}
	return lastColumnNumeric(table)
}

// validateDonut checks: exactly 2 columns, ≥2 rows, last column numeric.
func validateDonut(table *TableResult) bool {
	if len(table.Columns) != 2 || len(table.Rows) < 2 {
		return false
	}
	return lastColumnNumeric(table)
}

// validateMap dispatches by mode. Returns the result_type string or "".
func validateMap(op *querylang.MapOp, table *TableResult) string {
	switch op.Mode {
	case querylang.MapChoropleth:
		if validateChoropleth(op, table) {
			return "map-choropleth"
		}
	case querylang.MapScatter:
		if validateScatter(op, table) {
			return "map-scatter"
		}
	}
	return ""
}

// validateChoropleth checks that the country column exists and values look
// like ISO alpha-2 codes (2 uppercase letters). Empty values are allowed.
func validateChoropleth(op *querylang.MapOp, table *TableResult) bool {
	idx := columnIndex(table.Columns, op.CountryField)
	if idx < 0 {
		return false
	}
	if len(table.Rows) < 2 {
		return false
	}
	for _, row := range table.Rows {
		v := row[idx]
		if v == "" {
			continue
		}
		if !looksLikeISO2(v) {
			return false
		}
	}
	return true
}

// validateScatter checks that lat and lon columns exist and are numeric.
func validateScatter(op *querylang.MapOp, table *TableResult) bool {
	latIdx := columnIndex(table.Columns, op.LatField)
	lonIdx := columnIndex(table.Columns, op.LonField)
	if latIdx < 0 || lonIdx < 0 {
		return false
	}
	if len(table.Rows) < 2 {
		return false
	}
	for _, row := range table.Rows {
		if _, err := strconv.ParseFloat(row[latIdx], 64); err != nil {
			return false
		}
		if _, err := strconv.ParseFloat(row[lonIdx], 64); err != nil {
			return false
		}
	}
	return true
}

// lastColumnNumeric returns true if every row's last column is parseable as a float.
func lastColumnNumeric(table *TableResult) bool {
	last := len(table.Columns) - 1
	for _, row := range table.Rows {
		if _, err := strconv.ParseFloat(row[last], 64); err != nil {
			return false
		}
	}
	return true
}

// columnIndex returns the index of the named column, or -1 if not found.
func columnIndex(columns []string, name string) int {
	for i, c := range columns {
		if c == name {
			return i
		}
	}
	return -1
}

// looksLikeISO2 returns true if the string is exactly 2 uppercase ASCII letters.
func looksLikeISO2(s string) bool {
	if len(s) != 2 {
		return false
	}
	return s[0] >= 'A' && s[0] <= 'Z' && s[1] >= 'A' && s[1] <= 'Z'
}
