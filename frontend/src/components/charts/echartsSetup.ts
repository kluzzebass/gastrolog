/**
 * Tree-shaken ECharts setup — only register the modules we use.
 * Import the configured `echarts` instance from here, never from "echarts" directly.
 */
import * as echarts from "echarts/core";
import { BarChart, PieChart, LineChart, MapChart, ScatterChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
  GraphicComponent,
  VisualMapComponent,
  GeoComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  BarChart,
  PieChart,
  LineChart,
  MapChart,
  ScatterChart,
  GridComponent,
  TooltipComponent,
  LegendComponent,
  DataZoomComponent,
  GraphicComponent,
  VisualMapComponent,
  GeoComponent,
  CanvasRenderer,
]);



export * as echarts from "echarts/core";