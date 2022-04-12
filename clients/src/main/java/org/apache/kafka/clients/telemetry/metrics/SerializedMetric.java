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

package org.apache.kafka.clients.telemetry.metrics;

import io.opentelemetry.proto.metrics.v1.Metric;
import org.apache.kafka.clients.telemetry.MetricKey;

/**
 * In contrast to @{link io.confluent.telemetry.metrics.SinglePointMetric}, this is
 * an internal class that represents a metric that contains resource labels and
 * is ready to be handed off to the @link{io.confluent.telemetry.exporter.Exporter} objects.
 *
 * @see MetricKey
 */
public class SerializedMetric implements Keyed {

  private final Metric metric;
  private final MetricKey key;

  public SerializedMetric(Metric metric, MetricKey key) {
    this.metric = metric;
    this.key = key;
  }

  @Override
  public MetricKey key() {
    return key;
  }

  public Metric metric() {
    return metric;
  }
}
