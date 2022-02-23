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
import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the client host process metrics.
 */

public class HostProcessSensorRegistry extends AbstractSensorRegistry {

    private final static String GROUP_NAME = "host-process-telemetry";

    private final MetricName memoryBytes;

    private final MetricName cpuUserTime;

    private final MetricName cpuSystemTime;

    private final MetricName pid;

    public HostProcessSensorRegistry(Metrics metrics) {
        super(metrics);

        this.memoryBytes = createMetricName("memory.bytes",
            "Current process/runtime memory usage (RSS, not virtual).");
        this.cpuUserTime = createMetricName("cpu.user.time",
            "User CPU time used (seconds).");
        this.cpuSystemTime = createMetricName("cpu.system.time",
            "System CPU time used (seconds).");
        this.pid = createMetricName("pid",
            "The process id. Can be used, in conjunction with the client host name to map multiple client instances to the same process.");
    }

    public Sensor memoryBytes() {
        return gaugeSensor(memoryBytes);
    }

    public Sensor cpuUserTime() {
        return sumSensor(cpuUserTime);
    }

    public Sensor cpuSystemTime() {
        return sumSensor(cpuSystemTime);
    }

    public Sensor pid() {
        return sumSensor(pid);
    }

    private MetricName createMetricName(String unqualifiedName, String description) {
        return metrics.metricInstance(createTemplate(unqualifiedName, description, tags));
    }

    private MetricNameTemplate createTemplate(String unqualifiedName, String description, Set<String> tags) {
        String qualifiedName = String.format("org.apache.kafka.client.process.%s", unqualifiedName);
        return createTemplate(qualifiedName, GROUP_NAME, description, tags);
    }

}
