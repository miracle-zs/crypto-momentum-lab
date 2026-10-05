import * as echarts from "echarts/core";
import {
  AriaComponent,
  AxisPointerComponent,
  GridComponent,
  TooltipComponent,
} from "echarts/components";
import { LineChart } from "echarts/charts";
import { SVGRenderer } from "echarts/renderers";

// The dashboard only renders SVG line charts. Registering this explicit
// surface prevents unused chart types, map support, and Canvas rendering from
// becoming part of the operator console's initial download.
echarts.use([
  AriaComponent,
  AxisPointerComponent,
  GridComponent,
  LineChart,
  SVGRenderer,
  TooltipComponent,
]);

window.echarts = echarts;
