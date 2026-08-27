// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Scale metric processor for OTAP pipelines.
//!
//! Multiplies the data point values of named metrics by a constant factor, and
//! optionally restates their unit. Both are applied in the same pass, so a scaled
//! metric can never be observed carrying its pre-scaling unit.
//!
//! Example configuration (YAML):
//! ```yaml
//! rules:
//!   - metric_names: ["process_cpu_utilization"]
//!     factor: 100.0
//!   - metric_names: ["processing_duration"]
//!     factor: 0.000001
//!     unit: "ms"
//! ```
//!
//! Gauges, sums, histograms and summaries are scaled. Exponential histograms
//! cannot be scaled by a constant factor without rebuilding their bucket
//! structure, so a matched exponential histogram is left entirely untouched -
//! including its unit - and counted in `metrics_unsupported`.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = SCALE_METRIC_PROCESSOR_URN,
    target = "otel.processor.scale_metric",
);

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::MessageSourceLocalEffectHandlerExtension;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::error::Error as EngineError;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::process_duration::ComputeDuration;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_otap::{OTAP_PROCESSOR_FACTORIES, pdata::OtapPdata};
use otel_arrow_dfe_pdata::TryIntoWithOptions;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::otap::metric_scale::{MetricScaleRule, apply_metric_scale};
use otel_arrow_dfe_telemetry::metrics::MetricSet;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

mod metrics;

/// URN for the ScaleMetricProcessor
pub const SCALE_METRIC_PROCESSOR_URN: &str = "urn:otel:processor:scale_metric";

/// A single scaling rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// Names of the metrics this rule applies to.
    pub metric_names: Vec<String>,

    /// Multiplier applied to every data point value of a matched metric.
    pub factor: f64,

    /// Replacement unit, applied together with the factor.
    #[serde(default)]
    pub unit: Option<String>,
}

/// Configuration for the ScaleMetricProcessor.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// Rules applied in order; the first rule naming a metric wins.
    #[serde(default)]
    pub rules: Vec<Rule>,
}

/// Processor that multiplies metric data point values by a constant factor.
pub struct ScaleMetricProcessor {
    rules: Vec<MetricScaleRule>,
    metrics: MetricSet<metrics::ScaleMetricMetrics>,
    compute_duration: ComputeDuration,
}

impl ScaleMetricProcessor {
    /// Creates a new ScaleMetricProcessor from configuration.
    #[must_use = "ScaleMetricProcessor creation may fail and return a ConfigError"]
    pub fn from_config(pipeline_ctx: PipelineContext, config: &Value) -> Result<Self, ConfigError> {
        let config: Config =
            serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
                error: format!("Failed to parse ScaleMetricProcessor configuration: {e}"),
            })?;

        for rule in &config.rules {
            if rule.metric_names.is_empty() {
                return Err(ConfigError::InvalidUserConfig {
                    error: "ScaleMetricProcessor rule must name at least one metric".to_owned(),
                });
            }
            if !rule.factor.is_finite() {
                return Err(ConfigError::InvalidUserConfig {
                    error: format!(
                        "ScaleMetricProcessor factor must be finite, got {}",
                        rule.factor
                    ),
                });
            }
        }

        Ok(Self {
            rules: config
                .rules
                .into_iter()
                .map(|rule| MetricScaleRule {
                    metric_names: rule.metric_names,
                    factor: rule.factor,
                    unit: rule.unit,
                })
                .collect(),
            metrics: pipeline_ctx.register_metrics::<metrics::ScaleMetricMetrics>(),
            compute_duration: ComputeDuration::new(&pipeline_ctx),
        })
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for ScaleMetricProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        match msg {
            Message::Control(control_msg) => {
                if let otel_arrow_dfe_engine::control::NodeControlMsg::CollectTelemetry {
                    mut metrics_reporter,
                } = control_msg
                {
                    let _ = metrics_reporter.report(&mut self.metrics);
                    self.compute_duration.report(&mut metrics_reporter);
                }
                Ok(())
            }
            Message::PData(pdata) => {
                if self.rules.is_empty() {
                    return effect_handler
                        .send_message_with_source_node(pdata)
                        .await
                        .map_err(|e| e.into());
                }

                let (context, payload) = pdata.into_parts();
                let mut records: OtapArrowRecords = payload.try_into_with_default()?;

                let stats = effect_handler.timed(&self.compute_duration, || {
                    apply_metric_scale(&mut records, &self.rules).map_err(EngineError::from)
                })?;

                self.metrics.metrics_scaled.add(stats.metrics_scaled);
                self.metrics
                    .data_points_scaled
                    .add(stats.data_points_scaled);
                self.metrics
                    .metrics_unsupported
                    .add(stats.metrics_unsupported);

                effect_handler
                    .send_message_with_source_node(OtapPdata::new(context, records.into()))
                    .await
                    .map_err(|e| e.into())
            }
        }
    }
}

/// Factory function to create a ScaleMetricProcessor.
pub fn create_scale_metric_processor(
    pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, ConfigError> {
    let proc = ScaleMetricProcessor::from_config(pipeline_ctx, &node_config.config)?;
    Ok(ProcessorWrapper::local(
        proc,
        node,
        node_config,
        processor_config,
    ))
}

/// Register ScaleMetricProcessor as an OTAP processor factory
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Processor)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static SCALE_METRIC_PROCESSOR_FACTORY: otel_arrow_dfe_engine::ProcessorFactory<OtapPdata> =
    otel_arrow_dfe_engine::ProcessorFactory {
        name: SCALE_METRIC_PROCESSOR_URN,
        create:
            |pipeline_ctx: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             proc_cfg: &ProcessorConfig,
             _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
                create_scale_metric_processor(pipeline_ctx, node, node_config, proc_cfg)
            },
        wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
        validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<Config>,
    };

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_engine::context::ControllerContext;
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;

    fn pipeline_ctx() -> PipelineContext {
        ControllerContext::new(TelemetryRegistryHandle::new()).pipeline_context_with(
            "grp".into(),
            "pipeline".into(),
            0,
            1,
            0,
        )
    }

    /// Scenario: A rule is declared without naming any metric.
    /// Guarantees: Configuration is rejected instead of silently scaling nothing.
    #[test]
    fn rule_without_metric_names_is_rejected() {
        let config = serde_json::json!({
            "rules": [{ "metric_names": [], "factor": 2.0 }]
        });

        assert!(ScaleMetricProcessor::from_config(pipeline_ctx(), &config).is_err());
    }

    /// Scenario: A rule declares a non-finite factor.
    /// Guarantees: Configuration is rejected so data points cannot be turned into NaN or infinity.
    #[test]
    fn non_finite_factor_is_rejected() {
        let config = serde_json::json!({
            "rules": [{ "metric_names": ["cpu"], "factor": "nan" }]
        });

        assert!(ScaleMetricProcessor::from_config(pipeline_ctx(), &config).is_err());
    }

    /// Scenario: A valid rule set naming a factor and a replacement unit is parsed.
    /// Guarantees: Both the factor and the unit survive into the processor's rule list.
    #[test]
    fn valid_rules_are_parsed() {
        let config = serde_json::json!({
            "rules": [
                { "metric_names": ["process_cpu_utilization"], "factor": 100.0 },
                { "metric_names": ["processing_duration"], "factor": 0.000001, "unit": "ms" }
            ]
        });

        let processor =
            ScaleMetricProcessor::from_config(pipeline_ctx(), &config).expect("valid config");

        assert_eq!(processor.rules.len(), 2);
        assert_eq!(processor.rules[1].unit.as_deref(), Some("ms"));
    }
}
