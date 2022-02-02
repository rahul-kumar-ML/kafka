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

import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Sum;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import org.apache.kafka.common.MetricName;

public class OtlpTelemetrySerializer implements TelemetrySerializer {

    public void serialize(Map<MetricName, Long> values, OutputStream out) throws IOException {
        for (Map.Entry<MetricName, Long> entry : values.entrySet()) {
            MetricName name = entry.getKey();
            Long value = entry.getValue();

            NumberDataPoint numberDataPoint = NumberDataPoint.newBuilder()
                .setAsInt(value)
                .build();

            Sum sum = Sum.newBuilder()
                .addDataPoints(numberDataPoint)
                .build();

            Metric otlpMetric = Metric.newBuilder()
                .setName(name.name())
                .setDescription(name.description())
                .setSum(sum)
                .build();

            byte[] oltpBytes = otlpMetric.toByteArray();
            out.write(oltpBytes, 0, oltpBytes.length);
        }
    }

}
