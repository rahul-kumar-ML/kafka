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

import org.apache.kafka.clients.telemetry.metrics.Keyed;
import org.apache.kafka.clients.telemetry.metrics.SinglePointMetric;

public interface Emitter {

  /**
   * Tests whether this metric object could actually be emitted based on its key.
   * This is useful when materializing the metric object is an expensive operation.
   * @param key metric key to test against
   * @return true if it is possible for this object to get emitted, false otherwise
   */
  boolean shouldEmitMetric(Keyed key);

  /**
   * Emits a metric to all configured {@link io.confluent.telemetry.exporter.Exporter}
   * objects.
   * @param metric to emit
   * @return true if exported by at least one exporter, false otherwise
   */
  boolean emitMetric(SinglePointMetric metric);

}
