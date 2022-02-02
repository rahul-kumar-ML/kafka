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

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryManagementInterface implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TelemetryManagementInterface.class);

    private static final String CONTEXT = "kafka.telemetry";

    private static final String CLIENT_ID_METRIC_TAG = "client-id";

    private final Time time;

    private final String clientId;

    private final Metrics metrics;

    private final DeltaValueStore deltaValueStore;

    private final TelemetryMetricsReporter telemetryMetricsReporter;

    private final TelemetrySerializer telemetrySerializer;

    private final Object subscriptionLock = new Object();

    private TelemetrySubscription subscription;

    private final Object stateLock = new Object();

    private TelemetryState state = TelemetryState.initialized;

    public TelemetryManagementInterface(Time time, String clientId) {
        this.time = time;
        this.clientId = clientId;
        this.deltaValueStore = new DeltaValueStore();
        this.telemetryMetricsReporter = new TelemetryMetricsReporter(deltaValueStore);
        this.telemetrySerializer = new OtlpTelemetrySerializer();

        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig()
            .tags(metricsTags);
        MetricsContext metricsContext = new KafkaMetricsContext(CONTEXT);

        this.metrics = new Metrics(metricConfig,
            Collections.singletonList(telemetryMetricsReporter),
            time,
            metricsContext);
    }

    public Time time() {
        return time;
    }

    public String clientId() {
        return clientId;
    }

    public Metrics metrics() {
        return metrics;
    }

    @Override
    public void close() {
        log.trace("close");
        setState(TelemetryState.terminating);
    }

    public Bytes collectMetricsPayload(CompressionType compressionType,
        boolean deltaTemporality)
    throws IOException {
        log.trace("collectMetricsPayload starting");
        Collection<KafkaMetric> metrics = telemetryMetricsReporter.current();
        Map<MetricName, Long> values = new HashMap<>();

        for (KafkaMetric metric : metrics) {
            MetricName name = metric.metricName();
            long value = TelemetryUtils.metricValue(metric, deltaTemporality, deltaValueStore);
            log.debug("Including metric {} with value: {}", name.name(), value);
            values.put(name, value);
        }

        return TelemetryUtils.serialize(values, compressionType, telemetrySerializer);
    }

    public void setSubscription(TelemetrySubscription subscription) {
        synchronized (subscriptionLock) {
            this.subscription = subscription;
            subscriptionLock.notifyAll();
        }
    }

    public TelemetrySubscription subscription() {
        synchronized (subscriptionLock) {
            return subscription;
        }
    }

    public String clientInstanceId(Duration timeout) {
        synchronized (subscriptionLock) {
            if (subscription == null) {
                try {
                    subscriptionLock.wait(timeout.toMillis());
                } catch (InterruptedException e) {
                    // TODO: KIRK_TODO: figure out what we're supposed to do here.
                }
            }

            if (subscription == null) {
                // TODO: KIRK_TODO: verify this is correct and add some explanation.
                throw new IllegalTelemetryStateException();
            }

            return subscription.clientInstanceId().toString();
        }
    }

    public void setState(TelemetryState state) {
        synchronized (stateLock) {
            log.trace("Setting state from {} to {}", this.state, state);
            this.state = this.state.validateTransition(state);
        }
    }

    public TelemetryState state() {
        synchronized (stateLock) {
            return state;
        }
    }

    public long timeToNextUpdate() {
        long t = 0;
        TelemetryState s = state();

        if (s == TelemetryState.initialized || s == TelemetryState.subscription_needed) {
            // TODO: KIRK_TODO: verify
            t = 0;
        } else  if (s == TelemetryState.terminated) {
            // TODO: KIRK_TODO: verify and add a good error message
            throw new IllegalTelemetryStateException();
        } else {
            long milliseconds = time.milliseconds();

            synchronized (subscriptionLock) {
                if (subscription != null) {
                    long fetchMs = subscription.fetchMs();
                    long pushIntervalMs = subscription.pushIntervalMs();
                    t = fetchMs + pushIntervalMs - milliseconds;

                    if (t < 0)
                        t = 0;
                }
            }
        }

        log.debug("For state {}, returning {} for time to next update", s, t);
        return t;
    }

}
