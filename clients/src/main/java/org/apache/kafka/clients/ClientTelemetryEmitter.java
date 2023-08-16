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
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.kafka.common.telemetry.emitter.Context;
import org.apache.kafka.common.telemetry.emitter.Emitter;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;
import org.apache.kafka.common.telemetry.metrics.SinglePointMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTelemetryEmitter implements Emitter {

    private final static Logger log = LoggerFactory.getLogger(ClientTelemetryEmitter.class);

    private final Predicate<? super MetricKeyable> selector;
    private final Context context;
    private final List<SinglePointMetric> emitted;

    public ClientTelemetryEmitter(Predicate<? super MetricKeyable> selector, Context context) {
        this.selector = selector;
        this.context = context;
        this.emitted = new ArrayList<>();
    }

    @Override
    public boolean shouldEmitMetric(MetricKeyable metricKeyable) {
        log.info("[APM] - metricKeyable: " + metricKeyable.key());
        return true;
//        return selector.test(metricKeyable);
    }

    @Override
    public boolean emitMetric(SinglePointMetric metric) {
        emitted.add(metric);
        return true;
    }

    public List<SinglePointMetric> emitted() {
        return Collections.unmodifiableList(emitted);
    }

    public byte[] payload() {
        MetricsData.Builder builder = MetricsData.newBuilder();
        try {
            emitted.forEach(tm -> {
               Metric m = tm.metric().build();
                ResourceMetrics rm = context.buildMetric(m);
                builder.addResourceMetrics(rm);
            });
        } catch (Exception e) {
            log.error("Error constructing payload: ", e);
        }

        return builder.build().toByteArray();
    }
}
