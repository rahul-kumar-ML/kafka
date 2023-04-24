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
package org.apache.kafka.common.metrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.Server;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.IntStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricsGrpcClientTest {
    private MetricsGrpcClient client;
    private ManagedChannel channel;
    private Server server;
    private LinkedBlockingDeque<ExportMetricsServiceRequest> receivedRequests;

    @BeforeEach
    void setUp() throws IOException {
        receivedRequests = new LinkedBlockingDeque<>();

        // Server service to receive and queue ExportMetricsServiceRequest
        MetricsServiceGrpc.MetricsServiceImplBase receiverService = new MetricsServiceGrpc.MetricsServiceImplBase() {
            @Override
            public void export(ExportMetricsServiceRequest request,
                               StreamObserver<ExportMetricsServiceResponse> responseObserver) {

                receivedRequests.offer(request);
                responseObserver.onNext(ExportMetricsServiceResponse.getDefaultInstance());
                responseObserver.onCompleted();
            }
        };

        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(receiverService)
                .directExecutor()
                .build();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        server.start();

        client = new MetricsGrpcClient(channel);
    }

    @AfterEach
    void tearDown() {
        server.shutdown();
    }

    @Test
    void testInitializedGrpcClient() {
        assertEquals(client.getGrpcChannel(), channel);
        assertEquals(client.getEndpoint(), channel.authority());

        client.close();
    }

    @Test
    void testSuccessfulMetricsGrpcExport() {
        List<ResourceMetrics> singleMetric = Collections.singletonList(generateMetrics("test-metric1", 5));
        List<ResourceMetrics> multipleMetrics = new ArrayList<>(singleMetric);
        multipleMetrics.add(generateMetrics("test-metric2", 10));
        multipleMetrics.add(generateMetrics("test-metric3", 15));

        //client.initialize();
        client.export(multipleMetrics);
        client.export(singleMetric);

        assertFalse(receivedRequests.isEmpty());
        assertEquals(2, receivedRequests.size());

        ExportMetricsServiceRequest request1 = receivedRequests.poll();
        ExportMetricsServiceRequest request2 = receivedRequests.poll();
        validateSameExportRequest(multipleMetrics, request1);
        validateSameExportRequest(singleMetric, request2);


        client.close();
    }

    void validateSameExportRequest(List<ResourceMetrics> metrics, ExportMetricsServiceRequest request) {
        assertNotNull(request);
        assertEquals(metrics.size(), request.getResourceMetricsCount());
        assertEquals(metrics, request.getResourceMetricsList());

        IntStream.range(0, metrics.size()).forEach(i ->
                assertEquals(metrics.get(i), request.getResourceMetrics(i))
        );
    }

    @Test
    void testGrpcChannelClose() {
        //client.initialize();
        assertFalse(client.getGrpcChannel().isShutdown());

        client.close();
        assertTrue(client.getGrpcChannel().isShutdown());
        assertTrue(client.getGrpcChannel().isTerminated());
    }

    private ResourceMetrics generateMetrics(String name, int value) {
        return ResourceMetrics.newBuilder().addInstrumentationLibraryMetrics(
                InstrumentationLibraryMetrics.newBuilder().addMetrics(
                        Metric.newBuilder().setGauge(
                                Gauge.newBuilder().addDataPoints(
                                        NumberDataPoint.newBuilder().setAsInt(value).build()
                                ).build()
                        ).setName(name).build()
                ).build()
        ).build();
    }
}
