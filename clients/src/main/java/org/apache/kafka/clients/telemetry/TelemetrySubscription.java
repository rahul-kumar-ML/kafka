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
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.CompressionType;

public class TelemetrySubscription {

    private final long fetchMs;
    private final long throttleTimeMs;
    private final Uuid clientInstanceId;
    private final int subscriptionId;
    private final List<CompressionType> acceptedCompressionTypes;
    private final long pushIntervalMs;
    private final boolean deltaTemporality;
    private final Set<MetricName> metricNames;

    public TelemetrySubscription(long fetchMs,
        long throttleTimeMs,
        Uuid clientInstanceId,
        int subscriptionId,
        List<CompressionType> acceptedCompressionTypes,
        long pushIntervalMs,
        boolean deltaTemporality,
        Set<MetricName> metricNames) {
        this.fetchMs = fetchMs;
        this.throttleTimeMs = throttleTimeMs;
        this.clientInstanceId = clientInstanceId;
        this.subscriptionId = subscriptionId;
        this.acceptedCompressionTypes = Collections.unmodifiableList(acceptedCompressionTypes);
        this.pushIntervalMs = pushIntervalMs;
        this.deltaTemporality = deltaTemporality;
        this.metricNames = metricNames;
    }

    public long fetchMs() {
        return fetchMs;
    }

    public long throttleTimeMs() {
        return throttleTimeMs;
    }

    public Uuid clientInstanceId() {
        return clientInstanceId;
    }

    public int subscriptionId() {
        return subscriptionId;
    }

    public List<CompressionType> acceptedCompressionTypes() {
        return acceptedCompressionTypes;
    }

    public long pushIntervalMs() {
        return pushIntervalMs;
    }

    public boolean deltaTemporality() {
        return deltaTemporality;
    }

    public Set<MetricName> metricNames() {
        return metricNames;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", TelemetrySubscription.class.getSimpleName() + "[", "]")
            .add("fetchMs=" + fetchMs)
            .add("throttleTimeMs=" + throttleTimeMs)
            .add("clientInstanceId=" + clientInstanceId)
            .add("subscriptionId=" + subscriptionId)
            .add("acceptedCompressionTypes=" + acceptedCompressionTypes)
            .add("pushIntervalMs=" + pushIntervalMs)
            .add("deltaTemporality=" + deltaTemporality)
            .add("metricNames=" + metricNames)
            .toString();
    }
}
