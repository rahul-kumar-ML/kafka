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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;

public abstract class AbstractClientTelemetryRegistry {

    protected final Metrics metrics;

    protected final Set<String> tags;

    protected final List<MetricNameTemplate> allTemplates;

    protected AbstractClientTelemetryRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<>();
    }

//    public List<MetricNameTemplate> allTemplates() {
//        return allTemplates;
//    }

    public Sensor sensor(String name) {
        return metrics.sensor(name);
    }

    public void addMetric(MetricName m, Measurable measurable) {
        metrics.addMetric(m, measurable);
    }

//    public Sensor getSensor(String name) {
//        return metrics.getSensor(name);
//    }

//    protected Sensor sensor(MetricNameTemplate mnt,
//        Map<String, String> tags,
//        Supplier<MeasurableStat> measurableStatSupplier) {
//        MetricName mn = metrics.metricInstance(mnt, tags);
//        return sensor(mn, measurableStatSupplier);
//    }

    protected Sensor gaugeSensor(MetricName mn) {
        // TODO: KIRK_TODO: gauge...
        return sensor(mn, CumulativeSum::new);
    }

    protected Sensor gaugeSensor(MetricNameTemplate mnt, Map<String, String> tags) {
        MetricName mn = metrics.metricInstance(mnt, tags);
        return gaugeSensor(mn);
    }

    protected Sensor histogramSensor(MetricName mn) {
        // TODO: KIRK_TODO: histogram...
        return sensor(mn, CumulativeSum::new);
    }

    protected Sensor histogramSensor(MetricNameTemplate mnt, Map<String, String> tags) {
        MetricName mn = metrics.metricInstance(mnt, tags);
        return histogramSensor(mn);
    }

    protected Sensor stringSensor(MetricName mn) {
        // TODO: KIRK_TODO: string...
        return sensor(mn, CumulativeSum::new);
    }

    protected Sensor sumSensor(MetricName mn) {
        return sensor(mn, CumulativeSum::new);
    }

    protected Sensor sumSensor(MetricNameTemplate mnt, Map<String, String> tags) {
        MetricName mn = metrics.metricInstance(mnt, tags);
        return sumSensor(mn);
    }

    protected MetricNameTemplate createTemplate(String name, String group, String description, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, group, description, tags);
        allTemplates.add(template);
        return template;
    }

    private Sensor sensor(MetricName mn, Supplier<MeasurableStat> measurableStatSupplier) {
        Sensor sensor = sensor(mn.name());
        sensor.add(mn, measurableStatSupplier.get());
        return sensor;
    }

}
