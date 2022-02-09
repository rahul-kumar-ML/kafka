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

import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

public class ClientHostProcessTelemetryRegistry extends AbstractClientTelemetryRegistry {

    private final static String GROUP_NAME = "client-host-process-telemetry";

    public final MetricName memoryBytes;

    public final MetricName cpuUserTime;

    public final MetricName cpuSystemTime;

    public final MetricName ioWaitTime;

    public final MetricName pid;

    public ClientHostProcessTelemetryRegistry(Metrics metrics) {
        super(metrics);

        this.memoryBytes = createMetricName("memory.bytes",
            "Current process/runtime memory usage (RSS, not virtual).");
        this.cpuUserTime = createMetricName("cpu.user.time",
            "User CPU time used (seconds).");
        this.cpuSystemTime = createMetricName("cpu.system.time",
            "System CPU time used (seconds).");
        this.ioWaitTime = createMetricName("io.wait.time",
            "IO wait time (seconds).");
        this.pid = createMetricName("pid",
            "The process id. Can be used, in conjunction with the client host name to map multiple client instances to the same process.");
    }

    private MetricName createMetricName(String unqualifiedName, String description) {
        return metrics.metricInstance(createTemplate(unqualifiedName, description, tags));
    }

    private MetricNameTemplate createTemplate(String unqualifiedName, String description, Set<String> tags) {
        String qualifiedName = String.format("org.apache.kafka.client.process.%s", unqualifiedName);
        return createTemplate(qualifiedName, GROUP_NAME, description, tags);
    }

}
