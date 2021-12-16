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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TelemetryUtilsTest {

    @Test
    public void testLZ4CompressedSerialization() throws IOException {
        testSerialization(CompressionType.LZ4);
    }

    @Test
    public void testGzipCompressedSerialization() throws IOException {
        testSerialization(CompressionType.GZIP);
    }

    @Test
    public void testZstdCompressedSerialization() throws IOException {
        testSerialization(CompressionType.ZSTD);
    }

    @Test
    public void testSnappyCompressedSerialization() throws IOException {
        testSerialization(CompressionType.SNAPPY);
    }

    @Test
    public void testNoneCompressedSerialization() throws IOException {
        testSerialization(CompressionType.NONE);
    }

    private void testSerialization(CompressionType compressionType) throws IOException {
        TelemetryMetric telemetryMetric1 = telemetryMetric("test-1", 42);
        TelemetryMetric telemetryMetric2 = telemetryMetric("test-2", 123);
        StringTelemetrySerializer telemetrySerializer = new StringTelemetrySerializer();
        List<TelemetryMetric> telemetryMetrics = Arrays.asList(telemetryMetric1, telemetryMetric2);
        ByteBuffer compressed = TelemetryUtils.serialize(telemetryMetrics,
            compressionType,
            telemetrySerializer);

        ByteBuffer decompressed = ByteBuffer.allocate(10000);
        try (InputStream in = compressionType.wrapForInput(compressed, RecordBatch.CURRENT_MAGIC_VALUE, BufferSupplier.create())) {
            Utils.readFully(in, decompressed);
        }

        String s = new String(Utils.readBytes((ByteBuffer) decompressed.flip()));
        String expected = String.format("%s: %s\n%s: %s\n", telemetryMetric1.name(), telemetryMetric1.value(), telemetryMetric2.name(), telemetryMetric2.value());
        assertEquals(expected, s);
    }

    private TelemetryMetric telemetryMetric(String name, long value) {
        return new TelemetryMetric(name,
            MetricType.sum,
            value,
            "Description for " + name);
    }

}
