/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.telemetry.emitter;

import org.apache.kafka.clients.telemetry.Context;
import org.apache.kafka.clients.telemetry.exporter.Exporter;
import org.apache.kafka.clients.telemetry.metrics.Keyed;
import org.apache.kafka.clients.telemetry.metrics.SinglePointMetric;
import org.apache.kafka.clients.telemetry.reporter.TelemetryReporter;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;

import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Must construct after
 * 1. {@link TelemetryReporter#configure} [must be called first & only once]
 * 2. {@link TelemetryReporter#contextChange} [must be called second]
 */
public class TelemetryEmitter implements Emitter {

  private static final String METRICS_GROUP = "TelemetryEmitter";

  private final Context context;
  private final Supplier<Collection<Exporter>> exportersSupplier;
  private volatile Predicate<? super Keyed> metricsPredicate;

  /**
   * The total number of metrics that have been exported.
   *
   * Note: If a metric is exported by at least one exporter, it will be counted
   * as '+1' towards this counter.
   */
  private final Sensor exportedMetricsSensor;

  public TelemetryEmitter(
      Context context,
      Supplier<Collection<Exporter>> exportersSupplier,
      Predicate<? super Keyed> metricsPredicate,
      Metrics metrics
  ) {
    this.context = context;
    this.metricsPredicate = metricsPredicate;
    this.exportersSupplier = exportersSupplier;
    exportedMetricsSensor = metrics.sensor("exported-records");
    exportedMetricsSensor.add(new Meter(
        metrics.metricName(exportedMetricsSensor.name() + "-rate", METRICS_GROUP),
        metrics.metricName(exportedMetricsSensor.name() + "-total", METRICS_GROUP)
    ));
  }

  @Override
  public boolean shouldEmitMetric(Keyed key) {
    return metricsPredicate.test(key);
  }

  @Override
  public boolean emitMetric(SinglePointMetric metric) {
    /*
    SerializedMetric serializedMetric = metric.serialize(context);
    // If the metric was exported by at least one of the exporters this
    // should be true and we should increment our counter.
    boolean exported = false;
    for (Exporter e : exportersSupplier.get()) {
      exported |= e.emit(serializedMetric);
    }
    if (exported) {
      exportedMetricsSensor.record();
    }

    return exported;
    */
    return false;
  }

  public void reconfigurePredicate(Predicate<? super Keyed> metricsPredicate) {
    this.metricsPredicate = metricsPredicate;
  }

}
