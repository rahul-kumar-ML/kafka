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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

public class TelemetryUtilsTest {

    @Test
    public void testSerialization() throws IOException {
        Map<MetricName, Long> values = new HashMap<>();
        values.put(metricName("byte.size"), 1L);
        StringTelemetrySerializer telemetrySerializer = new StringTelemetrySerializer();
        Bytes bytes = TelemetryUtils.serialize(values, CompressionType.LZ4, telemetrySerializer);
        String s = new String(bytes.get());
        System.out.println("here's the result: " + s);
    }

    private MetricName metricName(String name) {
        return new MetricName(name,
            "test-metrics",
            "Description for " + name,
            Collections.emptyMap());
    }

}
