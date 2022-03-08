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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public interface MetricSelector {

    Collection<TelemetryMetric> filter(Collection<TelemetryMetric> metrics);

    MetricSelector NONE = new MetricSelector() {
        @Override
        public Collection<TelemetryMetric> filter(Collection<TelemetryMetric> metrics) {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return String.format("%s.NONE", MetricSelector.class.getSimpleName());
        }

    };

    MetricSelector ALL = new MetricSelector() {

        @Override
        public Collection<TelemetryMetric> filter(Collection<TelemetryMetric> metrics) {
            return Collections.unmodifiableCollection(metrics);
        }

        @Override
        public String toString() {
            return String.format("%s.ALL", MetricSelector.class.getSimpleName());
        }

    };

    class FilteredMetricSelector implements MetricSelector {

        private final Collection<String> filters;

        public FilteredMetricSelector(Collection<String> filters) {
            this.filters = filters;
        }

        @Override
        public Collection<TelemetryMetric> filter(Collection<TelemetryMetric> metrics) {
            // TODO: TELEMETRY_TODO: implement prefix string match more efficiently.
            List<TelemetryMetric> filtered = new ArrayList<>();

            for (String filter : filters) {
                for (TelemetryMetric telemetryMetric : metrics) {
                    if (telemetryMetric.name().startsWith(filter)) {
                        filtered.add(telemetryMetric);
                        break;
                    }
                }
            }

            return Collections.unmodifiableCollection(filtered);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", FilteredMetricSelector.class.getSimpleName() + "[", "]")
                .add("filters=" + filters)
                .toString();
        }

    }

}
