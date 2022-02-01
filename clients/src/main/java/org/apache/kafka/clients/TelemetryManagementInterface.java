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
package org.apache.kafka.clients;

import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Sum;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

public class TelemetryManagementInterface implements MetricsReporter {

    private static final String JMX_PREFIX = "kafka.consumer";

    private static final String CLIENT_ID_METRIC_TAG = "client-id";

    private final Map<MetricName, KafkaMetric> metricsMap = new HashMap<>();

    private final Map<MetricName, Long> previousValuesMap = new HashMap<>();

    private final Time time;

    private final String clientId;

    private final Metrics metrics;

    private final LogContext logContext;

    private final Object subscriptionLock = new Object();

    private TelemetrySubscription subscription;

    private volatile Logger log;

    private final Object stateLock = new Object();

    private TelemetryState state = TelemetryState.initialized;

    public TelemetryManagementInterface(Time time, String clientId, LogContext logContext) {
        this.time = time;
        this.clientId = clientId;
        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig()
            .tags(metricsTags);
        MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX);
        this.metrics = new Metrics(metricConfig,
            new ArrayList<>(0),
            time,
            metricsContext);


        this.logContext = logContext;
        updateLogger();

        // Do this last so we're fully backed before letting "this" escape...
        this.metrics.addReporter(this);
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
    public void configure(Map<String, ?> configs) {
        log.trace("configure - configs: {}", configs);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        log.trace("init - metrics: {}", metrics);
    }

    @Override
    public void close() {
        log.trace("close");
        setState(TelemetryState.terminating);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        if (metric.metricName().name().startsWith("org.apache.kafka")) {
            metricsMap.put(metric.metricName(), metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        if (metric.metricName().name().startsWith("org.apache.kafka")) {
            metricsMap.remove(metric.metricName());
            previousValuesMap.remove(metric.metricName());
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return MetricsReporter.super.reconfigurableConfigs();
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
        MetricsReporter.super.validateReconfiguration(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        MetricsReporter.super.reconfigure(configs);
    }

    @Override
    public void contextChange(MetricsContext metricsContext) {
        MetricsReporter.super.contextChange(metricsContext);
    }

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

    public byte[] collectMetricsPayload() {
        log.trace("collectMetricsPayload starting");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for (KafkaMetric metric : metricsMap.values()) {
            MetricName name = metric.metricName();
            Object value = metric.metricValue();
            log.debug("In collectMetricsPayload, including metric {} with value: {}", name.name(), value);

            double doubleValue = Double.parseDouble(value.toString());
            long longValue = Double.valueOf(doubleValue).longValue();
            Long previousValue = previousValuesMap.put(name, longValue);
            long deltaValue = previousValue != null ? longValue - previousValue : longValue;

            NumberDataPoint numberDataPoint = NumberDataPoint.newBuilder()
                .setAsInt(longValue)
                .build();

            Sum sum = Sum.newBuilder()
                .addDataPoints(numberDataPoint)
                .build();

            Metric otlpMetric = Metric.newBuilder()
                .setName(name.name())
                .setDescription(name.description())
                .setSum(sum)
                .build();

            byte[] bytes = otlpMetric.toByteArray();
            baos.write(bytes, 0, bytes.length);
        }

        return baos.toByteArray();
    }

    public void setSubscription(TelemetrySubscription subscription) {
        synchronized (subscriptionLock) {
            this.subscription = subscription;

            subscriptionLock.notifyAll();
        }

        updateLogger();
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
                    // TODO: KIRK_TODO: put on queue, wait for response
                }
            }

            if (subscription == null) {
                // we got a problem!
                throw new IllegalStateException();
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
            //
            t = 0;
        } else  if (s == TelemetryState.terminated) {
            throw new IllegalStateException();
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

    protected void updateLogger() {
        Object clientInstanceId;

        synchronized (subscriptionLock) {
            if (subscription != null)
                clientInstanceId = subscription.clientInstanceId();
            else
                clientInstanceId = "<not set>";
        }

        String prefix = String.format("%s[Telemetry clientId=%s, clientInstanceId=%s] ",
            logContext.logPrefix(),
            clientId,
            clientInstanceId);

        this.log = new LogContext(prefix).logger(TelemetryManagementInterface.class);
    }

}
