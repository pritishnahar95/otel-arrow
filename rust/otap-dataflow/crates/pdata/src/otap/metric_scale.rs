// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Multiplication of metric data point values by a constant factor.
//!
//! Metrics are selected by name in the root payload, then every value column of the
//! data points belonging to them is multiplied. The unit is restated in the same
//! pass, so a scaled metric can never be observed carrying its pre-scaling unit.
//!
//! Exponential histograms are not scalable by a constant factor without rebuilding
//! their bucket structure, so matched exponential histograms are left untouched and
//! counted separately rather than being partially rewritten.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt8Array, UInt16Array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};

use crate::OtapArrowRecords;
use crate::arrays::{NullableArrayAccessor, StringArrayAccessor, get_required_array};
use crate::error::{Error, Result};
use crate::otap::transform::transport_optimize::remove_transport_optimized_encodings;
use crate::otlp::metrics::MetricType;
use crate::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use crate::schema::consts;

/// Data point payloads whose values can be multiplied by a constant factor.
const SCALABLE_DATA_POINT_PAYLOADS: &[ArrowPayloadType] = &[
    ArrowPayloadType::NumberDataPoints,
    ArrowPayloadType::HistogramDataPoints,
    ArrowPayloadType::SummaryDataPoints,
];

/// A single scaling rule.
#[derive(Debug, Clone)]
pub struct MetricScaleRule {
    /// Metric names this rule applies to.
    pub metric_names: Vec<String>,
    /// Multiplier applied to every data point value of a matched metric.
    pub factor: f64,
    /// Replacement unit, applied in the same pass as the factor.
    pub unit: Option<String>,
}

/// Counts describing what a scaling pass changed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MetricScaleStats {
    /// Metrics matched by a rule and scaled.
    pub metrics_scaled: u64,
    /// Data point rows whose values were multiplied.
    pub data_points_scaled: u64,
    /// Metrics matched by a rule but left untouched because their type cannot be
    /// scaled by a constant factor.
    pub metrics_unsupported: u64,
}

/// Applies the supplied scaling rules to the metrics in `otap_batch`.
///
/// Batches that are not metrics, and metrics not named by any rule, are left
/// untouched.
pub fn apply_metric_scale(
    otap_batch: &mut OtapArrowRecords,
    rules: &[MetricScaleRule],
) -> Result<MetricScaleStats> {
    let mut stats = MetricScaleStats::default();
    if rules.is_empty() {
        return Ok(stats);
    }

    let root_payload_type = otap_batch.root_payload_type();
    if !matches!(
        root_payload_type,
        ArrowPayloadType::UnivariateMetrics | ArrowPayloadType::MultivariateMetrics
    ) {
        return Ok(stats);
    }

    let Some(root) = otap_batch.get(root_payload_type) else {
        return Ok(stats);
    };
    let root = remove_transport_optimized_encodings(root_payload_type, root)?;

    let plan = ScalePlan::build(&root, rules, &mut stats)?;
    if plan.factor_by_metric_id.is_empty() {
        return Ok(stats);
    }

    otap_batch.set(root_payload_type, plan.apply_units(&root)?)?;

    for payload_type in SCALABLE_DATA_POINT_PAYLOADS {
        let Some(data_points) = otap_batch.get(*payload_type) else {
            continue;
        };
        let data_points = remove_transport_optimized_encodings(*payload_type, data_points)?;

        let row_factors = plan.row_factors(&data_points)?;
        let scaled_rows = row_factors.iter().filter(|f| f.is_some()).count() as u64;
        if scaled_rows == 0 {
            continue;
        }

        otap_batch.set(
            *payload_type,
            scale_data_points(&data_points, &row_factors)?,
        )?;
        stats.data_points_scaled += scaled_rows;
    }

    Ok(stats)
}

/// Which metrics to scale, by how much, and what unit they end up with.
struct ScalePlan {
    factor_by_metric_id: HashMap<u16, f64>,
    /// Replacement unit per root row, `None` where the row keeps its unit.
    unit_by_row: Vec<Option<String>>,
}

impl ScalePlan {
    fn build(
        root: &RecordBatch,
        rules: &[MetricScaleRule],
        stats: &mut MetricScaleStats,
    ) -> Result<Self> {
        let ids = get_required_array(root, consts::ID)?
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| Error::ColumnDataTypeMismatch {
                name: consts::ID.into(),
                expect: DataType::UInt16,
                actual: root
                    .column_by_name(consts::ID)
                    .map_or(DataType::Null, |c| c.data_type().clone()),
            })?;
        let metric_types = get_required_array(root, consts::METRIC_TYPE)?
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| Error::ColumnDataTypeMismatch {
                name: consts::METRIC_TYPE.into(),
                expect: DataType::UInt8,
                actual: root
                    .column_by_name(consts::METRIC_TYPE)
                    .map_or(DataType::Null, |c| c.data_type().clone()),
            })?;
        let names = StringArrayAccessor::try_new(get_required_array(root, consts::NAME)?)?;

        let mut factor_by_metric_id = HashMap::new();
        let mut unit_by_row = vec![None; root.num_rows()];

        for (row, unit_slot) in unit_by_row.iter_mut().enumerate() {
            let Some(name) = names.value_at(row) else {
                continue;
            };
            let Some(rule) = rules
                .iter()
                .find(|r| r.metric_names.iter().any(|n| n == &name))
            else {
                continue;
            };

            if !is_scalable(metric_types.value(row)) {
                stats.metrics_unsupported += 1;
                continue;
            }

            let _ = factor_by_metric_id.insert(ids.value(row), rule.factor);
            *unit_slot = rule.unit.clone();
            stats.metrics_scaled += 1;
        }

        Ok(Self {
            factor_by_metric_id,
            unit_by_row,
        })
    }

    /// Returns the factor to apply to each data point row, keyed by its parent metric.
    fn row_factors(&self, data_points: &RecordBatch) -> Result<Vec<Option<f64>>> {
        let parent_ids = get_required_array(data_points, consts::PARENT_ID)?
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| Error::ColumnDataTypeMismatch {
                name: consts::PARENT_ID.into(),
                expect: DataType::UInt16,
                actual: data_points
                    .column_by_name(consts::PARENT_ID)
                    .map_or(DataType::Null, |c| c.data_type().clone()),
            })?
            .clone();

        Ok((0..parent_ids.len())
            .map(|row| {
                parent_ids
                    .is_valid(row)
                    .then(|| {
                        self.factor_by_metric_id
                            .get(&parent_ids.value(row))
                            .copied()
                    })
                    .flatten()
            })
            .collect())
    }

    /// Rewrites the unit column so a scaled metric never keeps its old unit.
    fn apply_units(&self, root: &RecordBatch) -> Result<RecordBatch> {
        if self.unit_by_row.iter().all(Option::is_none) {
            return Ok(root.clone());
        }

        let existing = root
            .column_by_name(consts::UNIT)
            .map(StringArrayAccessor::try_new)
            .transpose()?;
        let units: StringArray = (0..root.num_rows())
            .map(|row| {
                self.unit_by_row[row]
                    .clone()
                    .or_else(|| existing.as_ref().and_then(|a| a.value_at(row)))
            })
            .collect();

        let field = Arc::new(Field::new(consts::UNIT, DataType::Utf8, true));
        let mut fields = root.schema_ref().fields().to_vec();
        let mut columns = root.columns().to_vec();
        match root.schema_ref().index_of(consts::UNIT) {
            Ok(index) => {
                fields[index] = field;
                columns[index] = Arc::new(units);
            }
            Err(_) => {
                fields.push(field);
                columns.push(Arc::new(units));
            }
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|source| Error::ColumnLengthMismatch { source })
    }
}

/// Metric types whose data point values are a plain magnitude a factor can scale.
fn is_scalable(metric_type: u8) -> bool {
    matches!(
        MetricType::try_from(metric_type),
        Ok(MetricType::Gauge | MetricType::Sum | MetricType::Histogram | MetricType::Summary)
    )
}

/// Multiplies every value column of `data_points` by the per-row factor.
fn scale_data_points(
    data_points: &RecordBatch,
    row_factors: &[Option<f64>],
) -> Result<RecordBatch> {
    let mut columns = data_points.columns().to_vec();

    for (index, field) in data_points.schema_ref().fields().iter().enumerate() {
        let scaled = match field.name().as_str() {
            consts::INT_VALUE => scale_int_column(&columns[index], row_factors)?,
            // `sum`, `min`, `max` and `double_value` are all plain magnitudes.
            consts::DOUBLE_VALUE
            | consts::HISTOGRAM_SUM
            | consts::HISTOGRAM_MIN
            | consts::HISTOGRAM_MAX => scale_double_column(&columns[index], row_factors)?,
            consts::HISTOGRAM_EXPLICIT_BOUNDS => {
                scale_double_list_column(&columns[index], row_factors)?
            }
            consts::SUMMARY_QUANTILE_VALUES => {
                scale_quantile_list_column(&columns[index], row_factors)?
            }
            _ => continue,
        };
        columns[index] = scaled;
    }

    RecordBatch::try_new(data_points.schema(), columns)
        .map_err(|source| Error::ColumnLengthMismatch { source })
}

fn downcast<'a, T: Array + 'static>(
    array: &'a ArrayRef,
    name: &str,
    expect: DataType,
) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::ColumnDataTypeMismatch {
            name: name.into(),
            expect,
            actual: array.data_type().clone(),
        })
}

fn scale_double_column(array: &ArrayRef, row_factors: &[Option<f64>]) -> Result<ArrayRef> {
    let values = downcast::<Float64Array>(array, "double column", DataType::Float64)?;
    let scaled: Float64Array = (0..values.len())
        .map(|row| {
            values.is_valid(row).then(|| match row_factors[row] {
                Some(factor) => values.value(row) * factor,
                None => values.value(row),
            })
        })
        .collect();
    Ok(Arc::new(scaled))
}

/// Scales an integer column through `f64`, truncating toward zero like the
/// OpenTelemetry Collector's `scale_metric`.
fn scale_int_column(array: &ArrayRef, row_factors: &[Option<f64>]) -> Result<ArrayRef> {
    let values = downcast::<Int64Array>(array, consts::INT_VALUE, DataType::Int64)?;
    let scaled: Int64Array = (0..values.len())
        .map(|row| {
            values.is_valid(row).then(|| match row_factors[row] {
                Some(factor) => (values.value(row) as f64 * factor) as i64,
                None => values.value(row),
            })
        })
        .collect();
    Ok(Arc::new(scaled))
}

fn scale_double_list_column(array: &ArrayRef, row_factors: &[Option<f64>]) -> Result<ArrayRef> {
    let list = downcast::<ListArray>(
        array,
        consts::HISTOGRAM_EXPLICIT_BOUNDS,
        DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
    )?;
    let values = downcast::<Float64Array>(
        list.values(),
        consts::HISTOGRAM_EXPLICIT_BOUNDS,
        DataType::Float64,
    )?;

    let scaled = scale_values_by_list_row(values, list, row_factors);
    let DataType::List(item_field) = list.data_type() else {
        unreachable!("downcast to ListArray guarantees a List data type")
    };
    Ok(Arc::new(ListArray::new(
        item_field.clone(),
        list.offsets().clone(),
        Arc::new(scaled),
        list.nulls().cloned(),
    )))
}

fn scale_quantile_list_column(array: &ArrayRef, row_factors: &[Option<f64>]) -> Result<ArrayRef> {
    let list = downcast::<ListArray>(
        array,
        consts::SUMMARY_QUANTILE_VALUES,
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::empty()),
            false,
        ))),
    )?;
    let quantiles = downcast::<StructArray>(
        list.values(),
        consts::SUMMARY_QUANTILE_VALUES,
        DataType::Struct(Fields::empty()),
    )?;
    let value_index = quantiles
        .column_names()
        .iter()
        .position(|name| *name == consts::SUMMARY_VALUE)
        .ok_or_else(|| Error::ColumnNotFound {
            name: consts::SUMMARY_VALUE.into(),
        })?;
    let values = downcast::<Float64Array>(
        quantiles.column(value_index),
        consts::SUMMARY_VALUE,
        DataType::Float64,
    )?;

    let scaled = scale_values_by_list_row(values, list, row_factors);
    let mut children = quantiles.columns().to_vec();
    children[value_index] = Arc::new(scaled);
    let DataType::Struct(struct_fields) = quantiles.data_type() else {
        unreachable!("downcast to StructArray guarantees a Struct data type")
    };
    let quantiles =
        StructArray::try_new(struct_fields.clone(), children, quantiles.nulls().cloned())
            .map_err(|source| Error::ColumnLengthMismatch { source })?;

    let DataType::List(item_field) = list.data_type() else {
        unreachable!("downcast to ListArray guarantees a List data type")
    };
    Ok(Arc::new(ListArray::new(
        item_field.clone(),
        list.offsets().clone(),
        Arc::new(quantiles),
        list.nulls().cloned(),
    )))
}

/// Multiplies the child values of each list row by that row's factor, leaving the
/// offsets and null buffers of the list untouched.
fn scale_values_by_list_row(
    values: &Float64Array,
    list: &ListArray,
    row_factors: &[Option<f64>],
) -> Float64Array {
    let mut scaled: Vec<Option<f64>> = (0..values.len())
        .map(|i| values.is_valid(i).then(|| values.value(i)))
        .collect();

    let offsets = list.offsets();
    for (row, factor) in row_factors.iter().enumerate() {
        let Some(factor) = factor else { continue };
        if list.is_null(row) {
            continue;
        }
        for slot in &mut scaled[offsets[row] as usize..offsets[row + 1] as usize] {
            if let Some(value) = slot {
                *slot = Some(*value * factor);
            }
        }
    }

    Float64Array::from(scaled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otap::Metrics;
    use crate::schema::FieldExt;
    use arrow::array::{Float64Builder, ListBuilder, StructBuilder};
    use arrow::datatypes::{Fields, TimeUnit};

    fn id_field(name: &str) -> Field {
        Field::new(name, DataType::UInt16, true).with_plain_encoding()
    }

    fn ts_field(name: &str) -> Field {
        Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), false)
    }

    fn ts_array(len: usize) -> ArrayRef {
        Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
            0i64;
            len
        ]))
    }

    /// Builds a metrics root batch from (id, metric_type, name, unit) tuples.
    fn root_batch(rows: &[(u16, u8, &str, Option<&str>)]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(consts::ID, DataType::UInt16, true).with_plain_encoding(),
            Field::new(consts::METRIC_TYPE, DataType::UInt8, false),
            Field::new(consts::NAME, DataType::Utf8, false),
            Field::new(consts::UNIT, DataType::Utf8, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(rows.iter().map(|r| r.3).collect::<StringArray>()),
            ],
        )
        .expect("root batch")
    }

    fn number_dp_batch(rows: &[(u16, Option<i64>, Option<f64>)]) -> RecordBatch {
        let schema = Schema::new(vec![
            id_field(consts::PARENT_ID),
            ts_field(consts::TIME_UNIX_NANO),
            Field::new(consts::INT_VALUE, DataType::Int64, true),
            Field::new(consts::DOUBLE_VALUE, DataType::Float64, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.0))),
                ts_array(rows.len()),
                Arc::new(rows.iter().map(|r| r.1).collect::<Int64Array>()),
                Arc::new(rows.iter().map(|r| r.2).collect::<Float64Array>()),
            ],
        )
        .expect("number dp batch")
    }

    fn metrics(batches: Vec<(ArrowPayloadType, RecordBatch)>) -> OtapArrowRecords {
        let mut records = OtapArrowRecords::Metrics(Metrics::default());
        for (payload_type, batch) in batches {
            records.set(payload_type, batch).expect("set batch");
        }
        records
    }

    fn column<T: Array + Clone + 'static>(
        records: &OtapArrowRecords,
        payload_type: ArrowPayloadType,
        name: &str,
    ) -> T {
        records
            .get(payload_type)
            .expect("payload present")
            .column_by_name(name)
            .expect("column present")
            .as_any()
            .downcast_ref::<T>()
            .expect("column type")
            .clone()
    }

    fn rule(name: &str, factor: f64, unit: Option<&str>) -> MetricScaleRule {
        MetricScaleRule {
            metric_names: vec![name.to_owned()],
            factor,
            unit: unit.map(str::to_owned),
        }
    }

    /// Scenario: A gauge named by a rule carries both int and double data points.
    /// Guarantees: Both value columns are multiplied by the factor and int values truncate toward zero.
    #[test]
    fn number_data_points_are_scaled() {
        let mut records = metrics(vec![
            (
                ArrowPayloadType::UnivariateMetrics,
                root_batch(&[(1, 1, "cpu", Some("1"))]),
            ),
            (
                ArrowPayloadType::NumberDataPoints,
                number_dp_batch(&[(1, Some(7), None), (1, None, Some(0.25))]),
            ),
        ]);

        let stats = apply_metric_scale(&mut records, &[rule("cpu", 100.0, None)]).expect("scale");

        assert_eq!(stats.metrics_scaled, 1);
        assert_eq!(stats.data_points_scaled, 2);
        let ints = column::<Int64Array>(
            &records,
            ArrowPayloadType::NumberDataPoints,
            consts::INT_VALUE,
        );
        assert_eq!(ints.value(0), 700);
        let doubles = column::<Float64Array>(
            &records,
            ArrowPayloadType::NumberDataPoints,
            consts::DOUBLE_VALUE,
        );
        assert!((doubles.value(1) - 25.0).abs() < f64::EPSILON);
    }

    /// Scenario: A rule supplies a replacement unit alongside the factor.
    /// Guarantees: The unit column of the scaled metric is rewritten and unmatched metrics keep theirs.
    #[test]
    fn unit_is_replaced_for_scaled_metrics_only() {
        let mut records = metrics(vec![
            (
                ArrowPayloadType::UnivariateMetrics,
                root_batch(&[(1, 1, "duration", Some("ns")), (2, 1, "other", Some("By"))]),
            ),
            (
                ArrowPayloadType::NumberDataPoints,
                number_dp_batch(&[(1, None, Some(1000.0)), (2, None, Some(1000.0))]),
            ),
        ]);

        let stats = apply_metric_scale(&mut records, &[rule("duration", 0.000001, Some("ms"))])
            .expect("scale");

        assert_eq!(stats.data_points_scaled, 1);
        let units =
            column::<StringArray>(&records, ArrowPayloadType::UnivariateMetrics, consts::UNIT);
        assert_eq!(units.value(0), "ms");
        assert_eq!(units.value(1), "By");
        let doubles = column::<Float64Array>(
            &records,
            ArrowPayloadType::NumberDataPoints,
            consts::DOUBLE_VALUE,
        );
        assert!((doubles.value(0) - 0.001).abs() < 1e-12);
        assert!((doubles.value(1) - 1000.0).abs() < f64::EPSILON);
    }

    /// Scenario: A histogram is scaled by a factor.
    /// Guarantees: Sum, min, max and explicit bounds are all multiplied while counts are untouched.
    #[test]
    fn histogram_magnitudes_are_scaled() {
        let bounds_field = Arc::new(Field::new("item", DataType::Float64, true));
        let mut bounds = ListBuilder::new(Float64Builder::new()).with_field(bounds_field.clone());
        bounds.values().append_value(1.0);
        bounds.values().append_value(2.0);
        bounds.append(true);

        let schema = Schema::new(vec![
            id_field(consts::PARENT_ID),
            ts_field(consts::TIME_UNIX_NANO),
            Field::new(consts::HISTOGRAM_COUNT, DataType::UInt64, false),
            Field::new(consts::HISTOGRAM_SUM, DataType::Float64, true),
            Field::new(consts::HISTOGRAM_MIN, DataType::Float64, true),
            Field::new(consts::HISTOGRAM_MAX, DataType::Float64, true),
            Field::new(
                consts::HISTOGRAM_EXPLICIT_BOUNDS,
                DataType::List(bounds_field),
                true,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt16Array::from_iter_values([1u16])),
                ts_array(1),
                Arc::new(arrow::array::UInt64Array::from_iter_values([4u64])),
                Arc::new(Float64Array::from(vec![Some(10.0)])),
                Arc::new(Float64Array::from(vec![Some(1.0)])),
                Arc::new(Float64Array::from(vec![Some(5.0)])),
                Arc::new(bounds.finish()),
            ],
        )
        .expect("histogram batch");

        let mut records = metrics(vec![
            (
                ArrowPayloadType::UnivariateMetrics,
                root_batch(&[(1, 3, "latency", Some("ns"))]),
            ),
            (ArrowPayloadType::HistogramDataPoints, batch),
        ]);

        let stats = apply_metric_scale(&mut records, &[rule("latency", 2.0, None)]).expect("scale");

        assert_eq!(stats.data_points_scaled, 1);
        let sum = column::<Float64Array>(
            &records,
            ArrowPayloadType::HistogramDataPoints,
            consts::HISTOGRAM_SUM,
        );
        assert!((sum.value(0) - 20.0).abs() < f64::EPSILON);
        let counts = column::<arrow::array::UInt64Array>(
            &records,
            ArrowPayloadType::HistogramDataPoints,
            consts::HISTOGRAM_COUNT,
        );
        assert_eq!(counts.value(0), 4);
        let bounds = column::<ListArray>(
            &records,
            ArrowPayloadType::HistogramDataPoints,
            consts::HISTOGRAM_EXPLICIT_BOUNDS,
        );
        let values = bounds
            .value(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("bounds values")
            .clone();
        assert!((values.value(0) - 2.0).abs() < f64::EPSILON);
        assert!((values.value(1) - 4.0).abs() < f64::EPSILON);
    }

    /// Scenario: A summary metric with quantile values is scaled.
    /// Guarantees: The sum and each quantile value are multiplied while the quantile keys are unchanged.
    #[test]
    fn summary_quantile_values_are_scaled() {
        let struct_fields = Fields::from(vec![
            Field::new(consts::SUMMARY_QUANTILE, DataType::Float64, false),
            Field::new(consts::SUMMARY_VALUE, DataType::Float64, false),
        ]);
        let item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields.clone()),
            true,
        ));
        let mut quantiles = ListBuilder::new(StructBuilder::from_fields(struct_fields.clone(), 0))
            .with_field(item_field.clone());
        {
            let values = quantiles.values();
            values
                .field_builder::<Float64Builder>(0)
                .expect("quantile builder")
                .append_value(0.5);
            values
                .field_builder::<Float64Builder>(1)
                .expect("value builder")
                .append_value(3.0);
            values.append(true);
        }
        quantiles.append(true);

        let schema = Schema::new(vec![
            id_field(consts::PARENT_ID),
            ts_field(consts::TIME_UNIX_NANO),
            Field::new(consts::SUMMARY_COUNT, DataType::UInt64, false),
            Field::new(consts::SUMMARY_SUM, DataType::Float64, false),
            Field::new(
                consts::SUMMARY_QUANTILE_VALUES,
                DataType::List(item_field),
                true,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt16Array::from_iter_values([1u16])),
                ts_array(1),
                Arc::new(arrow::array::UInt64Array::from_iter_values([2u64])),
                Arc::new(Float64Array::from(vec![6.0])),
                Arc::new(quantiles.finish()),
            ],
        )
        .expect("summary batch");

        let mut records = metrics(vec![
            (
                ArrowPayloadType::UnivariateMetrics,
                root_batch(&[(1, 5, "duration", Some("ns"))]),
            ),
            (ArrowPayloadType::SummaryDataPoints, batch),
        ]);

        let stats =
            apply_metric_scale(&mut records, &[rule("duration", 0.5, None)]).expect("scale");

        assert_eq!(stats.data_points_scaled, 1);
        let sum = column::<Float64Array>(
            &records,
            ArrowPayloadType::SummaryDataPoints,
            consts::SUMMARY_SUM,
        );
        assert!((sum.value(0) - 3.0).abs() < f64::EPSILON);
        let list = column::<ListArray>(
            &records,
            ArrowPayloadType::SummaryDataPoints,
            consts::SUMMARY_QUANTILE_VALUES,
        );
        let entries = list.value(0);
        let entries = entries
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("quantile struct");
        let keys = entries
            .column_by_name(consts::SUMMARY_QUANTILE)
            .expect("quantile key")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("quantile key type");
        let values = entries
            .column_by_name(consts::SUMMARY_VALUE)
            .expect("quantile value")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("quantile value type");
        assert!((keys.value(0) - 0.5).abs() < f64::EPSILON);
        assert!((values.value(0) - 1.5).abs() < f64::EPSILON);
    }

    /// Scenario: A rule names an exponential histogram, which no constant factor can scale.
    /// Guarantees: The metric is counted as unsupported and neither its values nor its unit change.
    #[test]
    fn exponential_histograms_are_left_untouched() {
        let mut records = metrics(vec![(
            ArrowPayloadType::UnivariateMetrics,
            root_batch(&[(1, 4, "latency", Some("ns"))]),
        )]);

        let stats =
            apply_metric_scale(&mut records, &[rule("latency", 2.0, Some("ms"))]).expect("scale");

        assert_eq!(stats.metrics_unsupported, 1);
        assert_eq!(stats.metrics_scaled, 0);
        let units =
            column::<StringArray>(&records, ArrowPayloadType::UnivariateMetrics, consts::UNIT);
        assert_eq!(units.value(0), "ns");
    }

    /// Scenario: A rule set is applied to a batch whose root payload is not metrics.
    /// Guarantees: The batch is reported as unchanged rather than erroring.
    #[test]
    fn non_metric_batches_are_a_no_op() {
        let mut records = OtapArrowRecords::Logs(crate::otap::Logs::default());

        let stats = apply_metric_scale(&mut records, &[rule("cpu", 2.0, None)]).expect("scale");

        assert_eq!(stats, MetricScaleStats::default());
    }
}
