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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Histogram;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryUtils {

    private static final Logger log = LoggerFactory.getLogger(TelemetryUtils.class);

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

    public static List<CompressionType> acceptedCompressionTypes(List<Byte> acceptedCompressionTypes) {
        List<CompressionType> list = null;

        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            list = new ArrayList<>();

            for (Byte b : acceptedCompressionTypes) {
                int compressionId = b.intValue();

                try {
                    CompressionType compressionType = CompressionType.forId(compressionId);
                    list.add(compressionType);
                } catch (IllegalArgumentException e) {
                    log.warn("Accepted compression type with ID {} provided by broker is not a known compression type; ignoring", compressionId);
                }
            }
        }

        // If the set of accepted compression types provided by the server was empty or had
        // nothing valid in it, let's just return a non-null list, and we'll just end up using
        // no compression.
        if (list == null || list.isEmpty())
            list = Collections.emptyList();

        return list;
    }

    public static Uuid clientInstanceId(Uuid clientInstanceId) {
        return clientInstanceId.equals(Uuid.ZERO_UUID) ? Uuid.randomUuid() : clientInstanceId;
    }

    public static MetricType metricType(KafkaMetric kafkaMetric) {
        Measurable measurable = kafkaMetric.measurable();

        if (measurable instanceof Gauge) {
            return MetricType.gauge;
        } else if (measurable instanceof Histogram) {
            return MetricType.histogram;
        } else if (measurable instanceof CumulativeSum) {
            return MetricType.sum;
        } else {
            // TODO: KIRK_TODO: make message
            throw new InvalidMetricTypeException();
        }
    }

    public static ByteBuffer serialize(Collection<TelemetryMetric> telemetryMetrics,
        CompressionType compressionType,
        TelemetrySerializer telemetrySerializer)
        throws IOException {
        try (ByteBufferOutputStream compressedOut = new ByteBufferOutputStream(1024)) {
            try (OutputStream out = compressionType.wrapForOutput(compressedOut, RecordBatch.CURRENT_MAGIC_VALUE)) {
                telemetrySerializer.serialize(telemetryMetrics, out);
            }

            return (ByteBuffer) compressedOut.buffer().flip();
        }
    }

}
