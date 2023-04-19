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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PrototypeAsyncConsumerTest {

    private final static Optional<String> DEFAULT_GROUP_ID = Optional.of("group.id");
    private PrototypeAsyncConsumer<String, String> consumer;

    @AfterEach
    public void cleanup() {
        if (consumer != null)
            consumer.close(Duration.ZERO);
    }

    @Test
    public void testSuccessfulStartupShutdown() {
        consumer = newConsumer();
        assertDoesNotThrow(() -> consumer.close());
    }

    @Test
    public void testInvalidGroupId() {
        consumer = newConsumer(Optional.empty());
        assertThrows(InvalidGroupIdException.class, () -> consumer.committed(new HashSet<>()));
    }

    @Test
    public void testCommitAsync_NullCallback() throws InterruptedException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(100L));
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));

        PrototypeAsyncConsumer<?, ?> mockedConsumer = spy(newConsumer());
        doReturn(future).when(mockedConsumer).commit(offsets);
        mockedConsumer.commitAsync(offsets, null);
        future.complete(null);
        TestUtils.waitForCondition(future::isDone, 2000, "commit future should complete");

        assertFalse(future.isCompletedExceptionally());
    }

    @Test
    public void testCommitAsync_UserSuppliedCallback() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(100L));
        offsets.put(new TopicPartition("my-topic", 1), new OffsetAndMetadata(200L));

        PrototypeAsyncConsumer<?, ?> consumer = newConsumer();
        PrototypeAsyncConsumer<?, ?> mockedConsumer = spy(consumer);
        doReturn(future).when(mockedConsumer).commit(offsets);
        OffsetCommitCallback customCallback = mock(OffsetCommitCallback.class);
        mockedConsumer.commitAsync(offsets, customCallback);
        future.complete(null);
        verify(customCallback).onComplete(offsets, null);
    }

    @Test
    public void testCommitted() {
        Set<TopicPartition> mockTopicPartitions = mockTopicPartitionOffset().keySet();
        consumer = newConsumer();
        Map<TopicPartition, OffsetAndMetadata> result = consumer.committed(mockTopicPartitions, Duration.ofSeconds(1));
        assertEquals(mockTopicPartitions.size(), result.size());
    }

    @Test
    public void testFetch() {
        Set<TopicPartition> mockTopicPartitions = mockTopicPartitionOffset().keySet();
        consumer = newConsumer();
        ConsumerRecords<String, String> result = consumer.poll(Duration.ofSeconds(1));
        assertEquals(mockTopicPartitions.size(), result.count());
    }

    @Test
    public void testUnimplementedException() {
        consumer = newConsumer();
        assertThrows(KafkaException.class, consumer::assignment, "not implemented exception");
    }

    private HashMap<TopicPartition, OffsetAndMetadata> mockTopicPartitionOffset() {
        final TopicPartition t0 = new TopicPartition("t0", 2);
        final TopicPartition t1 = new TopicPartition("t0", 3);
        HashMap<TopicPartition, OffsetAndMetadata> topicPartitionOffsets = new HashMap<>();
        topicPartitionOffsets.put(t0, new OffsetAndMetadata(10L));
        topicPartitionOffsets.put(t1, new OffsetAndMetadata(20L));
        return topicPartitionOffsets;
    }

    private PrototypeAsyncConsumer<String, String> newConsumer() {
        return newConsumer(DEFAULT_GROUP_ID);
    }

    private PrototypeAsyncConsumer<String, String> newConsumer(final Optional<String> groupIdOpt) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        consumerProps.put(DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        groupIdOpt.ifPresent(groupId -> consumerProps.put(GROUP_ID_CONFIG, groupId));

        ConsumerConfig config = new ConsumerConfig(consumerProps);
        this.consumer = new PrototypeAsyncConsumer<>(new MockTime(),
                config,
                new StringDeserializer(),
                new StringDeserializer());
        return this.consumer;
    }
}

