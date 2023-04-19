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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.BlockingQueue;

public class PrototypeAsyncConsumerContext extends ConsumerContext {

    public final BlockingQueue<ApplicationEvent> applicationEventQueue;
    public final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    public PrototypeAsyncConsumerContext(LogContext logContext,
                                         Time time,
                                         BlockingQueue<ApplicationEvent> applicationEventQueue,
                                         BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        super(logContext, time);
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
    }
}
