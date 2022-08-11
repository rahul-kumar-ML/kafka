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

package org.apache.kafka.clients.consumer.channels;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.events.KafkaServerEvent;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class KafkaServerEventQueue {
    private static final int timeoutMs = 200; // TODO: get this from the config
    private LinkedBlockingDeque<KafkaServerEvent> queue;
    private final ConsumerConfig config;

    public KafkaServerEventQueue(final ConsumerConfig config) {
        this.queue = new LinkedBlockingDeque<>();
        this.config = config;
    }

    public Optional<KafkaServerEvent> poll() throws InterruptedException {
        return Optional.ofNullable(queue.poll(timeoutMs, TimeUnit.MILLISECONDS));
    }
    public Optional<KafkaServerEvent> peek() throws InterruptedException {
        Optional<KafkaServerEvent> event = Optional.ofNullable(queue.poll(timeoutMs, TimeUnit.MILLISECONDS));
        if(event.isPresent())
            queue.addFirst(event.get());
        return event;
    }

    public boolean isEmpty() {
        return this.queue.isEmpty();
    }

    public boolean enqueue(KafkaServerEvent event) {
        return queue.offer(event);
    }
}

