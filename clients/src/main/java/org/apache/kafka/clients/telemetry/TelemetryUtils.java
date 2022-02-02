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
package org.apache.kafka.clients.telemetry;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.record.CompressionType;

public class TelemetryUtils {

    public static Set<MetricName> metricNames(List<String> requestedMetrics) {
        Set<MetricName> set;

        if (requestedMetrics == null || requestedMetrics.isEmpty()) {
            // no metrics
            set = Collections.emptySet();
        } else if (requestedMetrics.size() == 1 && requestedMetrics.get(0).isEmpty()) {
            // TODO: KIRK_TODO: all metrics
            set = new HashSet<>();
        } else {
            // TODO: KIRK_TODO: prefix string match...
            set = new HashSet<>();
        }

        return set;
    }

    public static Set<CompressionType> acceptedCompressionTypes(List<Byte> acceptedCompressionTypes) {
        Set<CompressionType> set;

        if (acceptedCompressionTypes == null || acceptedCompressionTypes.isEmpty()) {
            set = Collections.emptySet();
        } else {
            set = new HashSet<>();
        }

        return set;
    }

    public static Uuid clientInstanceId(Uuid clientInstanceId) {
        return clientInstanceId.equals(Uuid.ZERO_UUID) ? Uuid.randomUuid() : clientInstanceId;
    }

    public static long metricValue(KafkaMetric metric,
        boolean deltaTemporality,
        DeltaValueStore deltaValueStore) {
        MetricName name = metric.metricName();
        Object value = metric.metricValue();

        double doubleValue = Double.parseDouble(value.toString());
        long longValue = Double.valueOf(doubleValue).longValue();

        if (metric.measurable() instanceof CumulativeSum && deltaTemporality) {
            Long previousValue = deltaValueStore.getAndSet(name, longValue);
            longValue = previousValue != null ? longValue - previousValue : longValue;
        }

        return longValue;
    }

}
