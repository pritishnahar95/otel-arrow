# Scale Metric Processor

URN: `urn:otel:processor:scale_metric`

Multiplies the data point values of named metrics by a constant factor, and
optionally restates their unit. The factor and the unit are applied in the same
pass, so a scaled metric is never observed carrying its pre-scaling unit.

## Configuration

```yaml
rules:
  - metric_names: ["process_cpu_utilization"]
    factor: 100.0
  - metric_names: ["processing_duration"]
    factor: 0.000001
    unit: "ms"
```

| Field | Required | Description |
| --- | --- | --- |
| `rules[].metric_names` | yes | Metric names the rule applies to. Must be non-empty. |
| `rules[].factor` | yes | Finite multiplier applied to every data point value. |
| `rules[].unit` | no | Replacement unit for the matched metrics. |

Rules are evaluated in order; the first rule naming a metric wins.

## Supported metric types

Gauges, sums, histograms and summaries are scaled. For histograms this covers
`sum`, `min`, `max` and `explicit_bounds`; bucket counts are magnitudes of
occurrence rather than of value, so they are left alone. For summaries the `sum`
and each quantile value are scaled, but the quantile keys are not.

Integer data points are scaled through `f64` and truncated toward zero.

Exponential histograms cannot be scaled by a constant factor without rebuilding
their bucket structure. A matched exponential histogram is therefore left
entirely untouched - including its unit - and counted in `metrics_unsupported`.

## Metrics

| Metric | Description |
| --- | --- |
| `processor.scale_metric.metrics_scaled` | Metrics matched by a rule and scaled. |
| `processor.scale_metric.data_points_scaled` | Data point rows whose values were multiplied. |
| `processor.scale_metric.metrics_unsupported` | Metrics matched by a rule but left untouched. |
