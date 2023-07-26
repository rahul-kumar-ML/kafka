/*
 * Copyright 2017-2020 Confluent Inc.
 */

package org.apache.kafka.common.telemetry.metrics;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.telemetry.MetricsUtils;

/**
 * This class represents a metric that does not yet contain resource labels.
 * These additional resource labels will be added as we hand it off to
 * the @link{io.confluent.telemetry.exporter.Exporter} objects.
 */
public class SinglePointMetric implements MetricKeyable {

  private final MetricKey key;
  private final Metric.Builder metricBuilder;

  private SinglePointMetric(MetricKey key,
                            Metric.Builder metricBuilder) {
    this.key = key;
    this.metricBuilder = metricBuilder;
  }

  @Override
  public MetricKey key() {
    return key;
  }

  public Metric.Builder metric() {
    return metricBuilder;
  }

  public static SinglePointMetric create(MetricKey metricKey, Metric.Builder metric) {
    return new SinglePointMetric(metricKey, metric);
  }

  private static NumberDataPoint.Builder point(Instant timestamp, Number value) {
    if (value instanceof Long || value instanceof Integer) {
      return point(timestamp, value.longValue());
    } else {
      return point(timestamp, value.doubleValue());
    }
  }

  private static NumberDataPoint.Builder point(Instant timestamp, long value) {
    return NumberDataPoint.newBuilder()
        .setTimeUnixNano(MetricsUtils.toTimeUnixNanos(timestamp))
        .setAsInt(value);
  }

  private static NumberDataPoint.Builder point(Instant timestamp, double value) {
    return NumberDataPoint.newBuilder()
        .setTimeUnixNano(MetricsUtils.toTimeUnixNanos(timestamp))
        .setAsDouble(value);
  }

  public static Iterable<KeyValue> asAttributes(Map<String, String> labels) {
    return labels.entrySet().stream().map(
        entry -> KeyValue.newBuilder()
            .setKey(entry.getKey())
            .setValue(AnyValue.newBuilder().setStringValue(entry.getValue())).build()
    )::iterator;
  }

  /**
   * creates a metric data point for arbitrary Number values.
   * Since OpenTelemetry only supports double and long primitive types, the following mapping applies
   * <p>
   * Long / Integer -> long
   * All other Number types -> double
   * <p>
   * For fine-grained control, use {@link #point(Instant, long)} and {@link #point(Instant, double)}
   *
   * @param metricKey
   * @param value
   * @param timestamp
   * @return
   */
  public static SinglePointMetric gauge(MetricKey metricKey,
                                        Number value,
                                        Instant timestamp) {
    NumberDataPoint.Builder point = point(timestamp, value);
    return gauge(metricKey, point);
  }

  public static SinglePointMetric gauge(MetricKey metricKey,
                                        long value,
                                        Instant timestamp) {
    NumberDataPoint.Builder point = point(timestamp, value);
    return gauge(metricKey, point);
  }

  public static SinglePointMetric gauge(MetricKey metricKey,
                                     double value,
                                     Instant timestamp) {
    NumberDataPoint.Builder point = point(timestamp, value);
    return gauge(metricKey, point);
  }

  private static SinglePointMetric gauge(MetricKey metricKey, NumberDataPoint.Builder point) {
    point.addAllAttributes(asAttributes(metricKey.tags()));

    Metric.Builder metric = Metric.newBuilder()
        .setName(metricKey.name());

    metric.getGaugeBuilder()
        .addDataPoints(point);
    return create(metricKey, metric);
  }

  public static SinglePointMetric deltaSum(MetricKey metricKey,
                                        long value,
                                        Instant timestamp,
                                        Instant startTimestamp) {
    NumberDataPoint.Builder point = point(timestamp, value)
        .setStartTimeUnixNano(MetricsUtils.toTimeUnixNanos(startTimestamp));

    return sum(metricKey, AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, point);
  }

  public static SinglePointMetric deltaSum(MetricKey metricKey,
                                        double value,
                                        Instant timestamp,
                                        Instant startTimestamp) {
    NumberDataPoint.Builder point = point(timestamp, value)
        .setStartTimeUnixNano(MetricsUtils.toTimeUnixNanos(startTimestamp));

    return sum(metricKey, AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, point);
  }


  public static SinglePointMetric sum(MetricKey metricKey, long value, Instant timestamp) {
    return sum(metricKey, value, timestamp, null);
  }


  public static SinglePointMetric sum(MetricKey metricKey, double value, Instant timestamp) {
    return sum(metricKey, value, timestamp, null);
  }

  public static SinglePointMetric sum(MetricKey metricKey,
                                      long value,
                                      Instant timestamp,
                                      Instant startTimestamp) {
    NumberDataPoint.Builder point = point(timestamp, value);
    if (startTimestamp != null) {
      point.setStartTimeUnixNano(MetricsUtils.toTimeUnixNanos(startTimestamp));
    }

    return sum(metricKey, AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, point);
  }

  public static SinglePointMetric sum(MetricKey metricKey,
                                      double value,
                                      Instant timestamp,
                                      Instant startTimestamp) {
    NumberDataPoint.Builder point = point(timestamp, value);
    if (startTimestamp != null) {
      point.setStartTimeUnixNano(MetricsUtils.toTimeUnixNanos(startTimestamp));
    }

    return sum(metricKey, AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, point);
  }

  private static SinglePointMetric sum(MetricKey metricKey,
                                       AggregationTemporality aggregationTemporality,
                                       NumberDataPoint.Builder point) {
    point.addAllAttributes(asAttributes(metricKey.tags()));
    Metric.Builder metric = Metric.newBuilder().setName(metricKey.name());

    metric
        .getSumBuilder()
        .setAggregationTemporality(aggregationTemporality)
        .addDataPoints(point);
    return create(metricKey, metric);
  }
}
