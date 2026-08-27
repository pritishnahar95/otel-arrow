// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the ScaleMetricProcessor node.

use otel_arrow_dfe_telemetry::instrument::Counter;
use otel_arrow_dfe_telemetry_macros::metric_set;

/// Metrics for the scale metric processor.
#[metric_set(name = "processor.scale_metric")]
#[derive(Debug, Default, Clone)]
pub struct ScaleMetricMetrics {
    /// Number of metrics matched by a rule and scaled.
    #[metric(unit = "{metric}")]
    pub metrics_scaled: Counter<u64>,

    /// Number of data point rows whose values were multiplied.
    #[metric(unit = "{data_point}")]
    pub data_points_scaled: Counter<u64>,

    /// Number of metrics matched by a rule but left untouched because their type
    /// cannot be scaled by a constant factor.
    #[metric(unit = "{metric}")]
    pub metrics_unsupported: Counter<u64>,
}
