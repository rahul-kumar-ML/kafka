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
import org.apache.kafka.common.KafkaException;
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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryUtils {

    private static final Logger log = LoggerFactory.getLogger(TelemetryUtils.class);

    public static Set<MetricName> validateMetricNames(List<String> requestedMetrics) {
        Set<MetricName> set;

        if (requestedMetrics == null || requestedMetrics.isEmpty()) {
            // no metrics
            set = Collections.emptySet();
        } else if (requestedMetrics.size() == 1 && requestedMetrics.get(0).isEmpty()) {
            // TODO: TELEMETRY_TODO: determine the set of all metrics
            set = new HashSet<>();
        } else {
            // TODO: TELEMETRY_TODO: prefix string match...
            set = new HashSet<>();
        }

        return set;
    }

    public static List<CompressionType> validateAcceptedCompressionTypes(List<Byte> acceptedCompressionTypes) {
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

    public static Uuid validateClientInstanceId(Uuid clientInstanceId) {
        return clientInstanceId.equals(Uuid.ZERO_UUID) ? Uuid.randomUuid() : clientInstanceId;
    }

    public static int validatePushInteravlMs(Integer pushIntervalMs) {
        // TODO: TELEMETRY_TODO: I don't think we'll need this, but maybe it's good to be paranoid?
        return pushIntervalMs != null && pushIntervalMs > 0 ? pushIntervalMs : 10000;
    }

    public static CompressionType preferredCompressionType(List<CompressionType> acceptedCompressionTypes) {
        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            // Broker is providing the compression types in order of preference. Grab the
            // first one.
            return acceptedCompressionTypes.get(0);
        } else {
            return CompressionType.NONE;
        }
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
            throw new InvalidMetricTypeException("Could not determine metric type from measurable type " + measurable + " of metric " + kafkaMetric);
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

    public static Bytes collectMetricsPayload(TelemetryManagementInterface tmi,
        CompressionType compressionType,
        boolean deltaTemporality) {
        try {
            ByteBuffer buf = tmi.collectMetricsPayload(compressionType, deltaTemporality);
            return Bytes.wrap(Utils.readBytes(buf));
        } catch (IOException e) {
            throw new TelemetrySerializationException("Couldn't serialize telemetry", e);
        }
    }

}
