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
package org.apache.kafka.clients.producer.internals;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.AbstractClientTelemetryRegistry;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

public class ProducerTopicTelemetryRegistry extends AbstractClientTelemetryRegistry {

    private static final String GROUP_NAME = "producer-topic-telemetry";

    private static final String ACKS_LABEL = "acks";

    private static final String PARTITION_LABEL = "partition";

    private static final String REASON_LABEL = "reason";

    private static final String TOPIC_LABEL = "topic";

    private final MetricNameTemplate queueBytes;

    private final MetricNameTemplate queueCount;

    private final MetricNameTemplate latency;

    private final MetricNameTemplate queueLatency;

    private final MetricNameTemplate recordRetries;

    private final MetricNameTemplate recordFailures;

    private final MetricNameTemplate recordSuccess;

    public ProducerTopicTelemetryRegistry(Metrics metrics) {
        super(metrics);

        Set<String> topicPartitionAcksTags = new LinkedHashSet<>(tags);
        topicPartitionAcksTags.add("topic");
        topicPartitionAcksTags.add("partition");
        topicPartitionAcksTags.add("acks");

        Set<String> topicPartitionAcksReasonTags = new LinkedHashSet<>(topicPartitionAcksTags);
        topicPartitionAcksReasonTags.add("reason");

        this.queueBytes = createTemplate("queue.bytes",
            "Number of bytes queued on partition queue.",
            topicPartitionAcksTags);
        this.queueCount = createTemplate("queue.count",
            "Number of records queued on partition queue.",
            topicPartitionAcksTags);
        this.latency = createTemplate("latency",
            "Total produce record latency, from application calling send()/produce() to ack received from broker.",
            topicPartitionAcksTags);
        this.queueLatency = createTemplate("queue.latency",
            "Time between send()/produce() and record being sent to broker.",
            topicPartitionAcksTags);
        this.recordRetries = createTemplate("record.retries",
            "Number of ProduceRequest retries.",
            topicPartitionAcksTags);
        this.recordFailures = createTemplate("record.failures",
            "Number of records that permanently failed delivery. Reason is a short string representation of the reason, which is typically the name of a Kafka protocol error code, e.g., “RequestTimedOut”.",
            topicPartitionAcksReasonTags);
        this.recordSuccess = createTemplate("record.success",
            "Number of records that have been successfully produced.",
            topicPartitionAcksTags);
    }

    public Sensor queueBytes(String topic, int partition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topic, partition, acks);
        return gaugeSensor(queueBytes, metricsTags);
    }

    public Sensor queueCount(String topic, int partition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topic, partition, acks);
        return gaugeSensor(queueCount, metricsTags);
    }

    public Sensor latency(String topic, int partition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topic, partition, acks);
        return histogramSensor(latency, metricsTags);
    }

    public Sensor queueLatency(String topic, int partition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topic, partition, acks);
        return histogramSensor(queueLatency, metricsTags);
    }

    public Sensor recordRetries(String topic, int partition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topic, partition, acks);
        return sumSensor(recordRetries, metricsTags);
    }

    public Sensor recordFailures(String topic, int partition, short acks, String reason) {
        Map<String, String> metricsTags = getMetricsTags(topic, partition, acks);
        metricsTags.put(REASON_LABEL, reason);
        return sumSensor(recordFailures, metricsTags);
    }

    public Sensor recordSuccess(String topic, int partition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topic, partition, acks);
        return sumSensor(recordSuccess, metricsTags);
    }

    private Map<String, String> getMetricsTags(String topic, int partition, short acks) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(ACKS_LABEL, formatAcks(String.valueOf(acks)));
        metricsTags.put(PARTITION_LABEL, String.valueOf(partition));
        metricsTags.put(TOPIC_LABEL, topic);
        return metricsTags;
    }

    private MetricNameTemplate createTemplate(String unqualifiedName, String description, Set<String> tags) {
        String qualifiedName = String.format("org.apache.kafka.client.producer.partition.%s", unqualifiedName);
        return createTemplate(qualifiedName, GROUP_NAME, description, tags);
    }

    static String formatAcks(String acks) {
        // TODO: KIRK_TODO: this mapping needs to be verified
        if (acks == null)
            return "all";

        acks = acks.trim();

        if (acks.equals("0") || acks.equalsIgnoreCase("none")) {
            return "none";
        } else if (acks.equals("1") || acks.equalsIgnoreCase("leader")) {
            return "leader";
        } else if (acks.equals("-1") || acks.equals("all")) {
            return "all";
        } else {
            return "all";
        }
    }

}
