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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

/**
 * Background thread runnable that consumes {@code ApplicationEvent} and
 * produces {@code BackgroundEvent}. It uses an event loop to consume and
 * produce events, and poll the network client to handle network IO.
 * <p>
 * It holds a reference to the {@link SubscriptionState}, which is
 * initialized by the polling thread.
 */
public class ConsumerBackgroundThread<K, V> extends KafkaThread implements AutoCloseable {

    private static final long MAX_POLL_TIMEOUT_MS = 5000;
    private static final String BACKGROUND_THREAD_NAME = "consumer_background_thread";

    private final Logger log;
    private final PrototypeAsyncConsumerContext consumerContext;
    private final ConsumerMetadata metadata;
    private final ApplicationEventProcessor<K, V> applicationEventProcessor;
    private final NetworkClientDelegate networkClientDelegate;
    private final GroupState groupState;
    private final RequestManagers<K, V> requestManagers;

    private boolean running;

    // Visible for testing
    ConsumerBackgroundThread(final PrototypeAsyncConsumerContext consumerContext,
                             final ConsumerMetadata metadata,
                             final GroupRebalanceConfig groupRebalanceConfig,
                             final ApplicationEventProcessor<K, V> processor,
                             final NetworkClientDelegate networkClient,
                             final RequestManagers<K, V> requestManagers) {
        super(BACKGROUND_THREAD_NAME, true);

        this.log = consumerContext.logContext.logger(getClass());
        this.consumerContext = consumerContext;
        this.applicationEventProcessor = processor;
        this.metadata = metadata;
        this.networkClientDelegate = networkClient;
        this.groupState = new GroupState(groupRebalanceConfig);
        this.requestManagers = requestManagers;
    }

    public ConsumerBackgroundThread(final PrototypeAsyncConsumerContext consumerContext,
                                    final ConsumerMetadata metadata,
                                    final GroupRebalanceConfig groupRebalanceConfig,
                                    final ConsumerConfig config,
                                    final SubscriptionState subscriptions,
                                    final FetchConfig<K, V> fetchConfig,
                                    final ApiVersions apiVersions,
                                    final Metrics metrics) {
        super(BACKGROUND_THREAD_NAME, true);

        try {
            this.log = consumerContext.logContext.logger(getClass());
            this.consumerContext = consumerContext;
            this.metadata = metadata;
            final FetchMetricsRegistry fetchMetricsRegistry = new FetchMetricsRegistry();
            final FetchMetricsManager fetchMetricsManager = new FetchMetricsManager(metrics, fetchMetricsRegistry);
            this.networkClientDelegate = NetworkClientDelegate.create(consumerContext,
                    config,
                    metrics,
                    metadata,
                    apiVersions,
                    fetchMetricsManager.throttleTimeSensor());
            this.groupState = new GroupState(groupRebalanceConfig);

            Long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            FetchRequestManager<K, V> fetchRequestManager = new FetchRequestManager<>(consumerContext,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    fetchMetricsManager,
                    retryBackoffMs);

            if (groupState.groupId != null) {
                CoordinatorRequestManager coordinatorManager = new CoordinatorRequestManager(consumerContext,
                        retryBackoffMs,
                        groupState.groupId);
                CommitRequestManager commitRequestManager = new CommitRequestManager(consumerContext,
                        subscriptions,
                        config,
                        coordinatorManager,
                        groupState);
                this.requestManagers = new RequestManagers<>(coordinatorManager,
                        commitRequestManager,
                        fetchRequestManager);
            } else {
                this.requestManagers = new RequestManagers<>(fetchRequestManager);
            }

            this.applicationEventProcessor = new ApplicationEventProcessor<>(consumerContext, requestManagers);
        } catch (final Exception e) {
            close();
            throw new KafkaException("Failed to construct background processor", e.getCause());
        }
    }

    @Override
    public void run() {
        running = true;

        try {
            log.debug("Background thread started");
            while (running) {
                try {
                    runOnce();
                    Utils.sleep(1000);
                } catch (final WakeupException e) {
                    log.debug("WakeupException caught, background thread won't be interrupted");
                    // swallow the wakeup exception to prevent killing the background thread.
                }
            }
        } catch (final Throwable t) {
            log.error("The background thread failed due to unexpected error", t);
        } finally {
            close();
            log.debug("Closed");
        }
    }

    /**
     * Poll and process an {@link ApplicationEvent}. It performs the following tasks:
     * 1. Drains and try to process all the requests in the queue.
     * 2. Iterate through the registry, poll, and get the next poll time for the network poll
     * 3. Poll the networkClient to send and retrieve the response.
     */
    void runOnce() {
        if (!consumerContext.applicationEventQueue.isEmpty()) {
            final Queue<ApplicationEvent> events = new LinkedList<>();
            consumerContext.applicationEventQueue.drainTo(events);

            for (ApplicationEvent event : events) {
                log.warn("Consuming application event: {}", event);
                Objects.requireNonNull(event);
                applicationEventProcessor.process(event);
            }
        }

        final long currentTimeMs = consumerContext.time.milliseconds();
        final long pollWaitTimeMs = requestManagers.entries().stream()
                .filter(Optional::isPresent)
                .map(m -> m.get().poll(currentTimeMs))
                .filter(Objects::nonNull)
                .map(this::handlePollResult)
                .reduce(MAX_POLL_TIMEOUT_MS, Math::min);
        networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs);
    }

    long handlePollResult(NetworkClientDelegate.PollResult res) {
        if (!res.unsentRequests.isEmpty()) {
            networkClientDelegate.addAll(res.unsentRequests);
        }
        return res.timeUntilNextPollMs;
    }

    public boolean isRunning() {
        return this.running;
    }

    public void wakeup() {
        networkClientDelegate.wakeup();
    }

    @Override
    public void close() {
        this.running = false;
        this.wakeup();
        Utils.closeQuietly(networkClientDelegate, "network client utils");
        Utils.closeQuietly(metadata, "consumer metadata client");
    }
}
