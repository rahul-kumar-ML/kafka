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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerBackgroundThreadTest {

    private static final long RETRY_BACKOFF_MS = 100;

    private PrototypeAsyncConsumerContext consumerContext;
    private ConsumerMetadata metadata;
    private NetworkClientDelegate networkClient;
    private ApplicationEventProcessor<String, String> processor;
    private final int requestTimeoutMs = 500;
    private GroupRebalanceConfig groupRebalanceConfig;
    private RequestManagers<String, String> requestManagers;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        this.consumerContext = new PrototypeAsyncConsumerContext(new LogContext(),
                new MockTime(0),
                new LinkedBlockingQueue<>(),
                new LinkedBlockingQueue<>());

        this.metadata = mock(ConsumerMetadata.class);
        this.networkClient = mock(NetworkClientDelegate.class);
        this.processor = mock(ApplicationEventProcessor.class);
        this.groupRebalanceConfig = new GroupRebalanceConfig(
                100,
                100,
                100,
                "group_id",
                Optional.empty(),
                RETRY_BACKOFF_MS,
                true);

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);

        ConsumerConfig config = new ConsumerConfig(properties);
        Metrics metrics = ConsumerUtils.metrics(config, consumerContext.time);

        CoordinatorRequestManager coordinatorRequestManager = new CoordinatorRequestManager(consumerContext,
                RETRY_BACKOFF_MS,
                "group_id");

        SubscriptionState subscriptions = ConsumerUtils.subscriptionState(config, consumerContext.logContext);
        CommitRequestManager commitRequestManager = new CommitRequestManager(consumerContext,
                subscriptions,
                config,
                coordinatorRequestManager,
                new GroupState(groupRebalanceConfig));
        FetchConfig<String, String> fetchConfig = ConsumerUtils.fetchConfig(config);
        FetchMetricsManager metricsManager = ConsumerUtils.fetchMetricsManager(metrics);
        FetchRequestManager<String, String> fetchRequestManager = new FetchRequestManager<>(consumerContext,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                RETRY_BACKOFF_MS);
        this.requestManagers = new RequestManagers<>(coordinatorRequestManager,
                commitRequestManager,
                fetchRequestManager);
    }

    @Test
    public void testStartupAndTearDown() {
        ConsumerBackgroundThread<String, String> backgroundThread = mockBackgroundThread();
        backgroundThread.start();
        assertTrue(backgroundThread.isRunning());
        backgroundThread.close();
    }

    @Test
    public void testApplicationEvent() {
        CoordinatorRequestManager coordinatorRequestManager = requestManagers.coordinatorRequestManager.get();
        CommitRequestManager commitRequestManager = requestManagers.commitRequestManager.get();
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitRequestManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        ConsumerBackgroundThread<String, String> backgroundThread = mockBackgroundThread();
        ApplicationEvent e = new NoopApplicationEvent("noop event");
        consumerContext.applicationEventQueue.add(e);
        backgroundThread.runOnce();
        verify(processor, times(1)).process(e);
        backgroundThread.close();
    }

    @Test
    void testFindCoordinator() {
        ConsumerBackgroundThread<String, String> backgroundThread = mockBackgroundThread();
        CoordinatorRequestManager coordinatorRequestManager = requestManagers.coordinatorRequestManager.get();
        CommitRequestManager commitRequestManager = requestManagers.commitRequestManager.get();
        when(coordinatorRequestManager.poll(anyLong())).thenReturn(mockPollCoordinatorResult());
        when(commitRequestManager.poll(anyLong())).thenReturn(mockPollCommitResult());
        backgroundThread.runOnce();
        Mockito.verify(coordinatorRequestManager, times(1)).poll(anyLong());
        Mockito.verify(networkClient, times(1)).poll(anyLong(), anyLong());
        backgroundThread.close();
    }

    @Test
    void testPollResultTimer() {
        try (ConsumerBackgroundThread<String, String> backgroundThread = mockBackgroundThread()) {
            // purposely setting a non MAX time to ensure it is returning Long.MAX_VALUE upon success
            NetworkClientDelegate.PollResult success = new NetworkClientDelegate.PollResult(
                    10,
                    Collections.singletonList(findCoordinatorUnsentRequest(requestTimeoutMs)));
            assertEquals(10, backgroundThread.handlePollResult(success));

            NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                    10,
                    new ArrayList<>());
            assertEquals(10, backgroundThread.handlePollResult(failure));
        }
    }

    private NetworkClientDelegate.UnsentRequest findCoordinatorUnsentRequest(final long timeout) {
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
            Optional.empty());
        req.setTimer(consumerContext.time, timeout);
        return req;
    }

    private ConsumerBackgroundThread<String, String> mockBackgroundThread() {

        return new ConsumerBackgroundThread<>(consumerContext,
                this.metadata,
                groupRebalanceConfig,
                this.processor,
                this.networkClient,
                requestManagers);
    }

    private NetworkClientDelegate.PollResult mockPollCoordinatorResult() {
        return new NetworkClientDelegate.PollResult(
                RETRY_BACKOFF_MS,
                Collections.singletonList(findCoordinatorUnsentRequest(requestTimeoutMs)));
    }

    private NetworkClientDelegate.PollResult mockPollCommitResult() {
        return new NetworkClientDelegate.PollResult(
                RETRY_BACKOFF_MS,
                Collections.singletonList(findCoordinatorUnsentRequest(requestTimeoutMs)));
    }
}
