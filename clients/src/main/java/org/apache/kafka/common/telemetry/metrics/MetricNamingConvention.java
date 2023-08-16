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
package org.apache.kafka.common.telemetry.metrics;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;

import com.google.common.base.Strings;
import java.util.regex.Pattern;
import org.apache.kafka.common.MetricName;

/**
 * This class encapsulates naming and mapping conventions defined as part of
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Metricsnamingandformat">Metrics naming and format</a>
 */
public class MetricNamingConvention {

    private static final String NAME_JOINER = ".";

    // remove kafka, metric or stats as these are redundant for KafkaExporter metrics
    private final static Pattern GROUP_PATTERN = Pattern.compile("\\.(metrics)");

    public static MetricNamingStrategy<MetricName> getMetricNamingStartegy(String domain) {
        return new MetricNamingStrategy<MetricName>() {
            @Override
            public MetricKey metricKey(MetricName metricName) {
                String group = Strings.nullToEmpty(metricName.group());
                String rawName = Strings.nullToEmpty(metricName.name());

                return new MetricKey(fullMetricName(domain, group, rawName));
            }

            @Override
            public MetricKey derivedMetricKey(MetricKey key, String derivedComponent) {
                return new MetricKey(key.getName() + NAME_JOINER + derivedComponent, key.tags());
            }
        };
    }

    /**
     * Creates a metric name given the domain, group, and name. The new String follows the following
     * conventions and rules:
     *
     * <ul>
     *   <li>domain is expected to be a host-name like value, e.g. {@code org.apache.kafka}</li>
     *   <li>group is cleaned of redundant words: "-metrics"</li>
     *   <li>the group and metric name is dot separated</li>
     *   <li>The name is created by joining the three components, e.g.:
     *     {@code org.apache.kafka.producer.connection.creation.rate}</li>
     * </ul>
     */
    private static String fullMetricName(String domain, String group, String name) {
        return domain
            + NAME_JOINER
            + cleanGroup(group)
            + NAME_JOINER
            + cleanMetric(name);
    }

    private static String cleanGroup(String group) {
        group = clean(group);
        return GROUP_PATTERN.matcher(group).replaceAll("");
    }

    private static String cleanMetric(String metric) {
        return clean(metric);
    }

    /**
     * This method maps a raw name to follow conventions and cleans up the result to be more legible:
     * - converts names to lower hyphen case conventions
     * - strips redundant parts of the metric name, such as -metrics
     * - normalizes artifacts of hyphen case to dot separated conversion
     */
    private static String clean(String raw) {
        // Convert from upper camel case to lower hyphen case
        String lowerHyphenCase = UPPER_CAMEL.to(LOWER_HYPHEN, raw);
        return lowerHyphenCase.replaceAll("-", NAME_JOINER);
    }
}
