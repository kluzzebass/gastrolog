package query

import (
	"testing"

	"gastrolog/internal/querylang"
)

func TestValidateBarchart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		table   *TableResult
		wantOK  bool
	}{
		{
			name: "valid 2 cols 3 rows",
			table: &TableResult{
				Columns: []string{"status", "count"},
				Rows:    [][]string{{"200", "50"}, {"404", "10"}, {"500", "5"}},
			},
			wantOK: true,
		},
		{
			name: "valid 3 cols",
			table: &TableResult{
				Columns: []string{"method", "status", "count"},
				Rows:    [][]string{{"GET", "200", "50"}, {"POST", "200", "30"}},
			},
			wantOK: true,
		},
		{
			name: "too few columns",
			table: &TableResult{
				Columns: []string{"count"},
				Rows:    [][]string{{"50"}, {"10"}},
			},
			wantOK: false,
		},
		{
			name: "too few rows",
			table: &TableResult{
				Columns: []string{"status", "count"},
				Rows:    [][]string{{"200", "50"}},
			},
			wantOK: false,
		},
		{
			name: "last col not numeric",
			table: &TableResult{
				Columns: []string{"status", "message"},
				Rows:    [][]string{{"200", "ok"}, {"404", "not found"}},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ValidateVizOp(&querylang.BarchartOp{}, tt.table)
			got := result != ""
			if got != tt.wantOK {
				t.Errorf("ValidateVizOp(barchart) = %q, wantOK=%v", result, tt.wantOK)
			}
			if got && result != "barchart" {
				t.Errorf("expected result_type 'barchart', got %q", result)
			}
		})
	}
}

func TestValidateDonut(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		table  *TableResult
		wantOK bool
	}{
		{
			name: "valid 2 cols 3 rows",
			table: &TableResult{
				Columns: []string{"level", "count"},
				Rows:    [][]string{{"info", "100"}, {"error", "20"}, {"warn", "15"}},
			},
			wantOK: true,
		},
		{
			name: "too many columns",
			table: &TableResult{
				Columns: []string{"a", "b", "c"},
				Rows:    [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
			},
			wantOK: false,
		},
		{
			name: "too few rows",
			table: &TableResult{
				Columns: []string{"level", "count"},
				Rows:    [][]string{{"info", "100"}},
			},
			wantOK: false,
		},
		{
			name: "last col not numeric",
			table: &TableResult{
				Columns: []string{"level", "label"},
				Rows:    [][]string{{"info", "high"}, {"error", "low"}},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ValidateVizOp(&querylang.DonutOp{}, tt.table)
			got := result != ""
			if got != tt.wantOK {
				t.Errorf("ValidateVizOp(donut) = %q, wantOK=%v", result, tt.wantOK)
			}
			if got && result != "donut" {
				t.Errorf("expected result_type 'donut', got %q", result)
			}
		})
	}
}

func TestValidateMapChoropleth(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		op     *querylang.MapOp
		table  *TableResult
		wantOK bool
	}{
		{
			name: "valid choropleth",
			op:   &querylang.MapOp{Mode: querylang.MapChoropleth, CountryField: "country"},
			table: &TableResult{
				Columns: []string{"country", "count"},
				Rows:    [][]string{{"US", "50"}, {"DE", "30"}, {"JP", "20"}},
			},
			wantOK: true,
		},
		{
			name: "valid with empty values",
			op:   &querylang.MapOp{Mode: querylang.MapChoropleth, CountryField: "country"},
			table: &TableResult{
				Columns: []string{"country", "count"},
				Rows:    [][]string{{"US", "50"}, {"", "30"}, {"JP", "20"}},
			},
			wantOK: true,
		},
		{
			name: "missing country column",
			op:   &querylang.MapOp{Mode: querylang.MapChoropleth, CountryField: "missing"},
			table: &TableResult{
				Columns: []string{"country", "count"},
				Rows:    [][]string{{"US", "50"}, {"DE", "30"}},
			},
			wantOK: false,
		},
		{
			name: "invalid ISO codes",
			op:   &querylang.MapOp{Mode: querylang.MapChoropleth, CountryField: "country"},
			table: &TableResult{
				Columns: []string{"country", "count"},
				Rows:    [][]string{{"United States", "50"}, {"Germany", "30"}},
			},
			wantOK: false,
		},
		{
			name: "too few rows",
			op:   &querylang.MapOp{Mode: querylang.MapChoropleth, CountryField: "country"},
			table: &TableResult{
				Columns: []string{"country", "count"},
				Rows:    [][]string{{"US", "50"}},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ValidateVizOp(tt.op, tt.table)
			got := result != ""
			if got != tt.wantOK {
				t.Errorf("ValidateVizOp(map choropleth) = %q, wantOK=%v", result, tt.wantOK)
			}
			if got && result != "map-choropleth" {
				t.Errorf("expected result_type 'map-choropleth', got %q", result)
			}
		})
	}
}

func TestValidateMapScatter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		op     *querylang.MapOp
		table  *TableResult
		wantOK bool
	}{
		{
			name: "valid scatter",
			op:   &querylang.MapOp{Mode: querylang.MapScatter, LatField: "lat", LonField: "lon"},
			table: &TableResult{
				Columns: []string{"lat", "lon", "count"},
				Rows:    [][]string{{"40.7", "-74.0", "50"}, {"51.5", "-0.1", "30"}},
			},
			wantOK: true,
		},
		{
			name: "missing lat column",
			op:   &querylang.MapOp{Mode: querylang.MapScatter, LatField: "missing", LonField: "lon"},
			table: &TableResult{
				Columns: []string{"lat", "lon", "count"},
				Rows:    [][]string{{"40.7", "-74.0", "50"}, {"51.5", "-0.1", "30"}},
			},
			wantOK: false,
		},
		{
			name: "missing lon column",
			op:   &querylang.MapOp{Mode: querylang.MapScatter, LatField: "lat", LonField: "missing"},
			table: &TableResult{
				Columns: []string{"lat", "lon", "count"},
				Rows:    [][]string{{"40.7", "-74.0", "50"}, {"51.5", "-0.1", "30"}},
			},
			wantOK: false,
		},
		{
			name: "non-numeric lat",
			op:   &querylang.MapOp{Mode: querylang.MapScatter, LatField: "lat", LonField: "lon"},
			table: &TableResult{
				Columns: []string{"lat", "lon", "count"},
				Rows:    [][]string{{"abc", "-74.0", "50"}, {"51.5", "-0.1", "30"}},
			},
			wantOK: false,
		},
		{
			name: "too few rows",
			op:   &querylang.MapOp{Mode: querylang.MapScatter, LatField: "lat", LonField: "lon"},
			table: &TableResult{
				Columns: []string{"lat", "lon", "count"},
				Rows:    [][]string{{"40.7", "-74.0", "50"}},
			},
			wantOK: false,
		},
		{
			name: "valid with empty lat/lon rows",
			op:   &querylang.MapOp{Mode: querylang.MapScatter, LatField: "lat", LonField: "lon"},
			table: &TableResult{
				Columns: []string{"lat", "lon", "count"},
				Rows: [][]string{
					{"40.7", "-74.0", "50"},
					{"", "", "38"},
					{"51.5", "-0.1", "30"},
				},
			},
			wantOK: true,
		},
		{
			name: "all rows empty lat/lon",
			op:   &querylang.MapOp{Mode: querylang.MapScatter, LatField: "lat", LonField: "lon"},
			table: &TableResult{
				Columns: []string{"lat", "lon", "count"},
				Rows:    [][]string{{"", "", "50"}, {"", "", "30"}},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ValidateVizOp(tt.op, tt.table)
			got := result != ""
			if got != tt.wantOK {
				t.Errorf("ValidateVizOp(map scatter) = %q, wantOK=%v", result, tt.wantOK)
			}
			if got && result != "map-scatter" {
				t.Errorf("expected result_type 'map-scatter', got %q", result)
			}
		})
	}
}

func TestValidateScatterPlot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		op     *querylang.ScatterOp
		table  *TableResult
		wantOK bool
	}{
		{
			name: "valid scatter",
			op:   &querylang.ScatterOp{XField: "latency", YField: "bytes"},
			table: &TableResult{
				Columns: []string{"host", "latency", "bytes"},
				Rows:    [][]string{{"a", "12.5", "1024"}, {"b", "8.3", "2048"}},
			},
			wantOK: true,
		},
		{
			name: "missing x column",
			op:   &querylang.ScatterOp{XField: "missing", YField: "bytes"},
			table: &TableResult{
				Columns: []string{"latency", "bytes"},
				Rows:    [][]string{{"12.5", "1024"}, {"8.3", "2048"}},
			},
			wantOK: false,
		},
		{
			name: "non-numeric values",
			op:   &querylang.ScatterOp{XField: "latency", YField: "bytes"},
			table: &TableResult{
				Columns: []string{"latency", "bytes"},
				Rows:    [][]string{{"fast", "1024"}, {"8.3", "2048"}},
			},
			wantOK: false,
		},
		{
			name: "too few rows",
			op:   &querylang.ScatterOp{XField: "latency", YField: "bytes"},
			table: &TableResult{
				Columns: []string{"latency", "bytes"},
				Rows:    [][]string{{"12.5", "1024"}},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ValidateVizOp(tt.op, tt.table)
			got := result != ""
			if got != tt.wantOK {
				t.Errorf("ValidateVizOp(scatter) = %q, wantOK=%v", result, tt.wantOK)
			}
			if got && result != "scatter" {
				t.Errorf("expected result_type 'scatter', got %q", result)
			}
		})
	}
}

func TestValidateLinechart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		table  *TableResult
		wantOK bool
	}{
		{
			name: "valid time series",
			table: &TableResult{
				Columns: []string{"_time", "count"},
				Rows:    [][]string{{"2026-01-01T00:00:00Z", "10"}, {"2026-01-01T01:00:00Z", "20"}},
			},
			wantOK: true,
		},
		{
			name: "valid multi-series",
			table: &TableResult{
				Columns: []string{"_time", "requests", "errors"},
				Rows:    [][]string{{"2026-01-01T00:00:00Z", "100", "5"}, {"2026-01-01T01:00:00Z", "200", "8"}},
			},
			wantOK: true,
		},
		{
			name: "first column not time",
			table: &TableResult{
				Columns: []string{"host", "count"},
				Rows:    [][]string{{"web-1", "10"}, {"web-2", "20"}},
			},
			wantOK: false,
		},
		{
			name: "too few rows",
			table: &TableResult{
				Columns: []string{"_time", "count"},
				Rows:    [][]string{{"2026-01-01T00:00:00Z", "10"}},
			},
			wantOK: false,
		},
		{
			name: "last column not numeric",
			table: &TableResult{
				Columns: []string{"_time", "status"},
				Rows:    [][]string{{"2026-01-01T00:00:00Z", "ok"}, {"2026-01-01T01:00:00Z", "fail"}},
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ValidateVizOp(&querylang.LinechartOp{}, tt.table)
			got := result != ""
			if got != tt.wantOK {
				t.Errorf("ValidateVizOp(linechart) = %q, wantOK=%v", result, tt.wantOK)
			}
			if got && result != "linechart" {
				t.Errorf("expected result_type 'linechart', got %q", result)
			}
		})
	}
}

func TestAutoDetectVizType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		table    *TableResult
		wantType string
	}{
		{
			name: "auto donut: 2 cols 3 rows numeric",
			table: &TableResult{
				Columns: []string{"level", "count"},
				Rows:    [][]string{{"info", "100"}, {"error", "20"}, {"warn", "15"}},
			},
			wantType: "donut",
		},
		{
			name: "no auto donut: 3 cols",
			table: &TableResult{
				Columns: []string{"a", "b", "count"},
				Rows:    [][]string{{"x", "y", "10"}, {"x", "z", "20"}},
			},
			wantType: "",
		},
		{
			name: "no auto donut: 1 row",
			table: &TableResult{
				Columns: []string{"level", "count"},
				Rows:    [][]string{{"info", "100"}},
			},
			wantType: "",
		},
		{
			name: "no auto donut: too many rows",
			table: &TableResult{
				Columns: []string{"status", "count"},
				Rows: func() [][]string {
					rows := make([][]string, 13)
					for i := range rows {
						rows[i] = []string{"s", "1"}
					}
					return rows
				}(),
			},
			wantType: "",
		},
		{
			name: "no auto donut: last col not numeric",
			table: &TableResult{
				Columns: []string{"level", "label"},
				Rows:    [][]string{{"info", "high"}, {"error", "low"}},
			},
			wantType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := AutoDetectVizType(tt.table)
			if got != tt.wantType {
				t.Errorf("AutoDetectVizType() = %q, want %q", got, tt.wantType)
			}
		})
	}
}
