// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Resolution of attribute values that live on a different payload than the one
//! being written.
//!
//! Copying an attribute from a resource or a scope onto a record is a join, not a
//! broadcast: each target row resolves to its own value. This module walks the
//! parent-id chain from the payload being written up to the root batch, then down
//! into the source attributes batch, and materializes one value per target row.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray, UInt32Array};
use arrow::compute::{cast, take};
use arrow::datatypes::DataType;

use crate::OtapArrowRecords;
use crate::arrays::{NullableArrayAccessor, StringArrayAccessor, get_required_array, get_u8_array};
use crate::error::{Error, Result};
use crate::otap::transform::transport_optimize::remove_transport_optimized_encodings;
use crate::otlp::attributes::AttributeValueType;
use crate::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use crate::schema::consts;

/// Where an attribute value is read from, relative to the record being written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttributeSourceScope {
    /// The resource the record belongs to.
    Resource,
    /// The instrumentation scope the record belongs to.
    Scope,
    /// The root record itself (log record, span, or metric).
    Record,
}

/// Resolves `source_key` for every row of `target_parent_payload_type`.
///
/// Returns an array with one entry per row of that payload's record batch, null
/// where the row has no value for the key. Returns `None` when the source cannot
/// be reached at all - a missing payload, an absent key, or a value type that
/// OTAP attributes cannot represent as a scalar column.
pub(super) fn resolve_source_values(
    otap_batch: &OtapArrowRecords,
    target_parent_payload_type: ArrowPayloadType,
    scope: AttributeSourceScope,
    source_key: &str,
) -> Result<Option<ArrayRef>> {
    let root_payload_type = otap_batch.root_payload_type();

    let Some(source_attrs_payload_type) = source_attrs_payload_type(scope, root_payload_type)
    else {
        return Ok(None);
    };
    let Some(source_attrs) = otap_batch.get(source_attrs_payload_type) else {
        return Ok(None);
    };
    let source_attrs =
        remove_transport_optimized_encodings(source_attrs_payload_type, source_attrs)?;
    let Some((values, row_by_source_id)) = index_source_rows(&source_attrs, source_key)? else {
        return Ok(None);
    };

    let Some(root_row_of_target_row) =
        map_rows_to_root(otap_batch, target_parent_payload_type, root_payload_type)?
    else {
        return Ok(None);
    };

    let Some(root_batch) = otap_batch.get(root_payload_type) else {
        return Ok(None);
    };
    let root_batch = remove_transport_optimized_encodings(root_payload_type, root_batch)?;
    let source_id_of_root_row = match scope {
        AttributeSourceScope::Resource => struct_ids(&root_batch, consts::RESOURCE)?,
        AttributeSourceScope::Scope => struct_ids(&root_batch, consts::SCOPE)?,
        AttributeSourceScope::Record => {
            ids_as_u64(get_required_array(&root_batch, consts::ID)?, consts::ID)?
        }
    };

    let indices: UInt32Array = root_row_of_target_row
        .iter()
        .map(|root_row| {
            let source_id = source_id_of_root_row.get((*root_row)?).copied().flatten()?;
            let row = row_by_source_id.get(&source_id)?;
            u32::try_from(*row).ok()
        })
        .collect();

    if indices.null_count() == indices.len() {
        return Ok(None);
    }

    let values =
        take(&values, &indices, None).map_err(|e| Error::ColumnLengthMismatch { source: e })?;
    Ok(Some(values))
}

/// The attributes payload holding the values for `scope`.
fn source_attrs_payload_type(
    scope: AttributeSourceScope,
    root_payload_type: ArrowPayloadType,
) -> Option<ArrowPayloadType> {
    match scope {
        AttributeSourceScope::Resource => Some(ArrowPayloadType::ResourceAttrs),
        AttributeSourceScope::Scope => Some(ArrowPayloadType::ScopeAttrs),
        AttributeSourceScope::Record => match root_payload_type {
            ArrowPayloadType::Logs => Some(ArrowPayloadType::LogAttrs),
            ArrowPayloadType::Spans => Some(ArrowPayloadType::SpanAttrs),
            ArrowPayloadType::UnivariateMetrics | ArrowPayloadType::MultivariateMetrics => {
                Some(ArrowPayloadType::MetricAttrs)
            }
            _ => None,
        },
    }
}

/// The payload a given payload's `parent_id` column points at.
fn parent_payload_type(
    payload_type: ArrowPayloadType,
    root_payload_type: ArrowPayloadType,
) -> Option<ArrowPayloadType> {
    match payload_type {
        ArrowPayloadType::NumberDataPoints
        | ArrowPayloadType::SummaryDataPoints
        | ArrowPayloadType::HistogramDataPoints
        | ArrowPayloadType::ExpHistogramDataPoints
        | ArrowPayloadType::SpanEvents
        | ArrowPayloadType::SpanLinks => Some(root_payload_type),
        ArrowPayloadType::NumberDpExemplars => Some(ArrowPayloadType::NumberDataPoints),
        ArrowPayloadType::HistogramDpExemplars => Some(ArrowPayloadType::HistogramDataPoints),
        ArrowPayloadType::ExpHistogramDpExemplars => Some(ArrowPayloadType::ExpHistogramDataPoints),
        _ => None,
    }
}

/// Maps each row of `payload_type` to the root batch row it descends from.
fn map_rows_to_root(
    otap_batch: &OtapArrowRecords,
    payload_type: ArrowPayloadType,
    root_payload_type: ArrowPayloadType,
) -> Result<Option<Vec<Option<usize>>>> {
    let Some(batch) = otap_batch.get(payload_type) else {
        return Ok(None);
    };

    if payload_type == root_payload_type {
        return Ok(Some((0..batch.num_rows()).map(Some).collect()));
    }

    let Some(parent_payload_type) = parent_payload_type(payload_type, root_payload_type) else {
        return Ok(None);
    };
    let Some(parent_batch) = otap_batch.get(parent_payload_type) else {
        return Ok(None);
    };
    let Some(root_row_of_parent_row) =
        map_rows_to_root(otap_batch, parent_payload_type, root_payload_type)?
    else {
        return Ok(None);
    };

    let parent_batch = remove_transport_optimized_encodings(parent_payload_type, parent_batch)?;
    let parent_row_by_id = id_index(&ids_as_u64(
        get_required_array(&parent_batch, consts::ID)?,
        consts::ID,
    )?);

    let batch = remove_transport_optimized_encodings(payload_type, batch)?;
    let parent_ids = ids_as_u64(
        get_required_array(&batch, consts::PARENT_ID)?,
        consts::PARENT_ID,
    )?;

    Ok(Some(
        parent_ids
            .iter()
            .map(|parent_id| {
                let parent_row = parent_row_by_id.get(&(*parent_id)?)?;
                root_row_of_parent_row.get(*parent_row).copied().flatten()
            })
            .collect(),
    ))
}

/// Finds the rows of an attributes batch carrying `source_key`, indexed by parent id.
///
/// A key may legitimately appear with different value types across rows. Only the
/// type of the first occurrence is copied, because the destination is a single
/// typed column.
fn index_source_rows(
    source_attrs: &arrow::array::RecordBatch,
    source_key: &str,
) -> Result<Option<(ArrayRef, HashMap<u64, usize>)>> {
    let keys =
        StringArrayAccessor::try_new(get_required_array(source_attrs, consts::ATTRIBUTE_KEY)?)?;
    let types = get_u8_array(source_attrs, consts::ATTRIBUTE_TYPE)?;
    let parent_ids = ids_as_u64(
        get_required_array(source_attrs, consts::PARENT_ID)?,
        consts::PARENT_ID,
    )?;

    let mut value_type = None;
    let mut row_by_source_id = HashMap::new();
    for (row, parent_id) in parent_ids.iter().enumerate() {
        if keys.value_at(row).as_deref() != Some(source_key) {
            continue;
        }
        let row_type = AttributeValueType::try_from(types.value(row)).map_err(|_| {
            Error::UnexpectedRecordBatchState {
                reason: format!("unknown attribute value type {}", types.value(row)),
            }
        })?;
        match value_type {
            None => value_type = Some(row_type),
            Some(expected) if expected == row_type => {}
            Some(_) => continue,
        }
        let Some(parent_id) = *parent_id else {
            continue;
        };
        let _ = row_by_source_id.insert(parent_id, row);
    }

    let Some(column_name) = value_type.and_then(scalar_value_column) else {
        return Ok(None);
    };
    let Some(values) = source_attrs.column_by_name(column_name) else {
        return Ok(None);
    };

    // The destination column is plain, so dictionary-encoded sources are decoded here
    // rather than leaving a dictionary to be unified against unrelated values.
    let values = match values.data_type() {
        DataType::Dictionary(_, value_type) => {
            cast(values, value_type).map_err(|e| Error::ColumnLengthMismatch { source: e })?
        }
        _ => Arc::clone(values),
    };

    Ok(Some((values, row_by_source_id)))
}

/// The value column holding a given attribute type, or `None` if it is not a scalar.
const fn scalar_value_column(value_type: AttributeValueType) -> Option<&'static str> {
    match value_type {
        AttributeValueType::Str => Some(consts::ATTRIBUTE_STR),
        AttributeValueType::Int => Some(consts::ATTRIBUTE_INT),
        AttributeValueType::Double => Some(consts::ATTRIBUTE_DOUBLE),
        AttributeValueType::Bool => Some(consts::ATTRIBUTE_BOOL),
        AttributeValueType::Bytes => Some(consts::ATTRIBUTE_BYTES),
        AttributeValueType::Empty | AttributeValueType::Map | AttributeValueType::Slice => None,
    }
}

fn struct_ids(batch: &arrow::array::RecordBatch, struct_column: &str) -> Result<Vec<Option<u64>>> {
    let Some(ids) = batch
        .column_by_name(struct_column)
        .and_then(|column| column.as_any().downcast_ref::<StructArray>())
        .and_then(|struct_array| struct_array.column_by_name(consts::ID).cloned())
    else {
        return Ok(vec![None; batch.num_rows()]);
    };

    ids_as_u64(&ids, consts::ID)
}

fn ids_as_u64(array: &ArrayRef, name: &str) -> Result<Vec<Option<u64>>> {
    let array = match array.data_type() {
        DataType::Dictionary(_, value_type) => {
            cast(array, value_type).map_err(|e| Error::ColumnLengthMismatch { source: e })?
        }
        _ => Arc::clone(array),
    };

    let widened = match array.data_type() {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            cast(&array, &DataType::UInt64)
                .map_err(|e| Error::ColumnLengthMismatch { source: e })?
        }
        actual => {
            return Err(Error::ColumnDataTypeMismatch {
                name: name.into(),
                expect: DataType::UInt64,
                actual: actual.clone(),
            });
        }
    };

    let widened = widened
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        // safety: we have just cast to UInt64
        .expect("can downcast to u64");

    Ok(widened.iter().collect())
}

fn id_index(ids: &[Option<u64>]) -> HashMap<u64, usize> {
    ids.iter()
        .enumerate()
        .filter_map(|(row, id)| id.map(|id| (id, row)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::otap::transform::{
        AttributeAssignment, AttributeCondition, AttributeValueSource, AttributesTransform,
        InsertTransform, LiteralValue, UpsertTransform, apply_attribute_transform,
    };
    use crate::proto::OtlpProtoMessage;
    use crate::proto::opentelemetry::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use crate::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
    use crate::proto::opentelemetry::metrics::v1::{
        Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use crate::proto::opentelemetry::resource::v1::Resource;
    use crate::testing::round_trip::{otap_to_otlp, otlp_to_otap};

    fn from_attribute(scope: AttributeSourceScope, key: &str) -> AttributeValueSource {
        AttributeValueSource::FromAttribute {
            scope,
            key: key.to_owned(),
        }
    }

    fn sources(entries: &[(&str, AttributeValueSource)]) -> BTreeMap<String, AttributeValueSource> {
        entries
            .iter()
            .map(|(key, source)| ((*key).to_owned(), source.clone()))
            .collect()
    }

    fn logs(
        resource_attributes: Vec<KeyValue>,
        scope_attributes: Vec<KeyValue>,
        log_records: Vec<LogRecord>,
    ) -> OtapArrowRecords {
        otlp_to_otap(&OtlpProtoMessage::Logs(LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        attributes: scope_attributes,
                        ..Default::default()
                    }),
                    log_records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }))
    }

    fn log_attributes(otap_batch: &OtapArrowRecords) -> Vec<Vec<KeyValue>> {
        let OtlpProtoMessage::Logs(logs_data) = otap_to_otlp(otap_batch) else {
            panic!("expected logs")
        };
        logs_data.resource_logs[0].scope_logs[0]
            .log_records
            .iter()
            .map(|record| record.attributes.clone())
            .collect()
    }

    fn value_of(attributes: &[KeyValue], key: &str) -> Option<AnyValue> {
        attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.clone())
    }

    /// Scenario: A resource attribute is inserted onto every log record under that resource.
    /// Guarantees: Each record receives the resource's value rather than a broadcast constant.
    #[test]
    fn resource_attribute_is_copied_onto_records() {
        let mut otap_batch = logs(
            vec![KeyValue::new(
                "service.instance.id",
                AnyValue::new_string("instance-1"),
            )],
            vec![],
            vec![
                LogRecord::build().event_name("a").finish(),
                LogRecord::build().event_name("b").finish(),
            ],
        );

        let _ = apply_attribute_transform(
            &mut otap_batch,
            ArrowPayloadType::LogAttrs,
            &AttributesTransform::default().with_insert(InsertTransform::with_sources(sources(&[
                (
                    "instanceId",
                    from_attribute(AttributeSourceScope::Resource, "service.instance.id"),
                ),
            ]))),
            false,
        )
        .expect("transform");

        for attributes in log_attributes(&otap_batch) {
            assert_eq!(
                value_of(&attributes, "instanceId"),
                Some(AnyValue::new_string("instance-1"))
            );
        }
    }

    /// Scenario: A scope attribute is inserted onto records, one of which already has the key.
    /// Guarantees: Insert leaves the pre-existing value alone, which is the fallback precedence
    /// the OTTL config hand-rolls with a nil check.
    #[test]
    fn insert_does_not_overwrite_an_existing_key() {
        let mut otap_batch = logs(
            vec![],
            vec![KeyValue::new(
                "custom.componentName",
                AnyValue::new_string("from-scope"),
            )],
            vec![
                LogRecord::build()
                    .event_name("a")
                    .attributes(vec![KeyValue::new(
                        "componentName",
                        AnyValue::new_string("explicit"),
                    )])
                    .finish(),
                LogRecord::build().event_name("b").finish(),
            ],
        );

        let _ = apply_attribute_transform(
            &mut otap_batch,
            ArrowPayloadType::LogAttrs,
            &AttributesTransform::default().with_insert(InsertTransform::with_sources(sources(&[
                (
                    "componentName",
                    from_attribute(AttributeSourceScope::Scope, "custom.componentName"),
                ),
            ]))),
            false,
        )
        .expect("transform");

        let attributes = log_attributes(&otap_batch);
        assert_eq!(
            value_of(&attributes[0], "componentName"),
            Some(AnyValue::new_string("explicit"))
        );
        assert_eq!(
            value_of(&attributes[1], "componentName"),
            Some(AnyValue::new_string("from-scope"))
        );
    }

    /// Scenario: An upsert sources a scope attribute that only some records can resolve.
    /// Guarantees: Resolvable records are overwritten and the rest keep their original value.
    #[test]
    fn upsert_overwrites_only_where_the_source_resolves() {
        let mut otap_batch = logs(
            vec![],
            vec![KeyValue::new(
                "custom.componentName",
                AnyValue::new_string("from-scope"),
            )],
            vec![
                LogRecord::build()
                    .event_name("a")
                    .attributes(vec![KeyValue::new(
                        "componentName",
                        AnyValue::new_string("explicit"),
                    )])
                    .finish(),
            ],
        );

        let _ = apply_attribute_transform(
            &mut otap_batch,
            ArrowPayloadType::LogAttrs,
            &AttributesTransform::default().with_upsert(UpsertTransform::with_sources(sources(&[
                (
                    "componentName",
                    from_attribute(AttributeSourceScope::Scope, "custom.componentName"),
                ),
            ]))),
            false,
        )
        .expect("transform");

        assert_eq!(
            value_of(&log_attributes(&otap_batch)[0], "componentName"),
            Some(AnyValue::new_string("from-scope"))
        );
    }

    /// Scenario: The source key is absent from the batch.
    /// Guarantees: Nothing is written, rather than an empty or null attribute appearing.
    #[test]
    fn a_missing_source_key_writes_nothing() {
        let mut otap_batch = logs(
            vec![KeyValue::new("other", AnyValue::new_string("x"))],
            vec![],
            vec![LogRecord::build().event_name("a").finish()],
        );

        let _ = apply_attribute_transform(
            &mut otap_batch,
            ArrowPayloadType::LogAttrs,
            &AttributesTransform::default().with_insert(InsertTransform::with_sources(sources(&[
                (
                    "instanceId",
                    from_attribute(AttributeSourceScope::Resource, "service.instance.id"),
                ),
            ]))),
            false,
        )
        .expect("transform");

        assert_eq!(
            value_of(&log_attributes(&otap_batch)[0], "instanceId"),
            None
        );
    }

    /// Scenario: A resource attribute is inserted onto metric data point attributes.
    /// Guarantees: The value is resolved across two parent-id hops, data point to metric to
    /// resource, which is the join the internal telemetry pipeline depends on.
    #[test]
    fn resource_attribute_is_copied_onto_metric_data_points() {
        let mut otap_batch = otlp_to_otap(&OtlpProtoMessage::Metrics(MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue::new(
                        "microsoft.pipeline.resourceId",
                        AnyValue::new_string("/subscriptions/x"),
                    )],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric::build()
                            .name("cpu")
                            .data_gauge(Gauge::new(vec![
                                NumberDataPoint::build()
                                    .time_unix_nano(1u64)
                                    .value_double(1.0)
                                    .finish(),
                                NumberDataPoint::build()
                                    .time_unix_nano(2u64)
                                    .value_double(2.0)
                                    .finish(),
                            ]))
                            .finish(),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }));

        let _ = apply_attribute_transform(
            &mut otap_batch,
            ArrowPayloadType::NumberDpAttrs,
            &AttributesTransform::default().with_insert(InsertTransform::with_sources(sources(&[
                (
                    "resourceId",
                    from_attribute(
                        AttributeSourceScope::Resource,
                        "microsoft.pipeline.resourceId",
                    ),
                ),
            ]))),
            false,
        )
        .expect("transform");

        let OtlpProtoMessage::Metrics(metrics_data) = otap_to_otlp(&otap_batch) else {
            panic!("expected metrics")
        };
        let metric = &metrics_data.resource_metrics[0].scope_metrics[0].metrics[0];
        let Some(crate::proto::opentelemetry::metrics::v1::metric::Data::Gauge(gauge)) =
            &metric.data
        else {
            panic!("expected gauge")
        };
        assert_eq!(gauge.data_points.len(), 2);
        for data_point in &gauge.data_points {
            assert_eq!(
                value_of(&data_point.attributes, "resourceId"),
                Some(AnyValue::new_string("/subscriptions/x"))
            );
        }
    }

    /// Scenario: A conditional insert copies `node.id` only where `node.type` is `receiver`.
    /// Guarantees: Records under a non-matching scope are untouched, which is the guard the
    /// OTTL config expresses with `where instrumentation_scope.attributes["node.type"] == ...`.
    #[test]
    fn a_condition_restricts_the_insert_to_matching_records() {
        let transform = || {
            AttributesTransform::default().with_insert(InsertTransform::with_assignments(
                [(
                    "componentName".to_owned(),
                    AttributeAssignment::when(
                        AttributeValueSource::FromAttribute {
                            scope: AttributeSourceScope::Scope,
                            key: "node.id".to_owned(),
                        },
                        AttributeCondition {
                            scope: AttributeSourceScope::Scope,
                            key: "node.type".to_owned(),
                            equals: LiteralValue::Str("receiver".to_owned()),
                        },
                    ),
                )]
                .into(),
            ))
        };

        let mut matching = logs(
            vec![],
            vec![
                KeyValue::new("node.id", AnyValue::new_string("otlp-in")),
                KeyValue::new("node.type", AnyValue::new_string("receiver")),
            ],
            vec![LogRecord::build().event_name("a").finish()],
        );
        let _ = apply_attribute_transform(
            &mut matching,
            ArrowPayloadType::LogAttrs,
            &transform(),
            false,
        )
        .expect("transform");
        assert_eq!(
            value_of(&log_attributes(&matching)[0], "componentName"),
            Some(AnyValue::new_string("otlp-in"))
        );

        let mut non_matching = logs(
            vec![],
            vec![
                KeyValue::new("node.id", AnyValue::new_string("batch")),
                KeyValue::new("node.type", AnyValue::new_string("processor")),
            ],
            vec![LogRecord::build().event_name("a").finish()],
        );
        let _ = apply_attribute_transform(
            &mut non_matching,
            ArrowPayloadType::LogAttrs,
            &transform(),
            false,
        )
        .expect("transform");
        assert_eq!(
            value_of(&log_attributes(&non_matching)[0], "componentName"),
            None
        );
    }

    /// Scenario: A conditional upsert would overwrite an attribute the records already carry.
    /// Guarantees: The existing value survives where the condition fails, so a conditional
    /// upsert is a no-op rather than a blanket overwrite.
    #[test]
    fn a_failing_condition_leaves_an_existing_value_alone() {
        let mut otap_batch = logs(
            vec![],
            vec![KeyValue::new("node.type", AnyValue::new_string("exporter"))],
            vec![
                LogRecord::build()
                    .event_name("a")
                    .attributes(vec![KeyValue::new(
                        "componentName",
                        AnyValue::new_string("original"),
                    )])
                    .finish(),
            ],
        );

        let _ = apply_attribute_transform(
            &mut otap_batch,
            ArrowPayloadType::LogAttrs,
            &AttributesTransform::default().with_upsert(UpsertTransform::with_assignments(
                [(
                    "componentName".to_owned(),
                    AttributeAssignment::when(
                        AttributeValueSource::Literal(LiteralValue::Str("replaced".to_owned())),
                        AttributeCondition {
                            scope: AttributeSourceScope::Scope,
                            key: "node.type".to_owned(),
                            equals: LiteralValue::Str("receiver".to_owned()),
                        },
                    ),
                )]
                .into(),
            )),
            false,
        )
        .expect("transform");

        assert_eq!(
            value_of(&log_attributes(&otap_batch)[0], "componentName"),
            Some(AnyValue::new_string("original"))
        );
    }

    /// Scenario: A condition tests an attribute that does not exist anywhere in the batch.
    /// Guarantees: The action is skipped entirely rather than treated as vacuously true.
    #[test]
    fn a_condition_on_a_missing_attribute_never_holds() {
        let mut otap_batch = logs(
            vec![],
            vec![],
            vec![LogRecord::build().event_name("a").finish()],
        );

        let _ = apply_attribute_transform(
            &mut otap_batch,
            ArrowPayloadType::LogAttrs,
            &AttributesTransform::default().with_insert(InsertTransform::with_assignments(
                [(
                    "componentName".to_owned(),
                    AttributeAssignment::when(
                        AttributeValueSource::Literal(LiteralValue::Str("x".to_owned())),
                        AttributeCondition {
                            scope: AttributeSourceScope::Scope,
                            key: "node.type".to_owned(),
                            equals: LiteralValue::Str("receiver".to_owned()),
                        },
                    ),
                )]
                .into(),
            )),
            false,
        )
        .expect("transform");

        assert_eq!(
            value_of(&log_attributes(&otap_batch)[0], "componentName"),
            None
        );
    }
}
