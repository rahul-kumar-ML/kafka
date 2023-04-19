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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class RequestManagers<K, V> {

    public final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    public final Optional<CommitRequestManager> commitRequestManager;
    public final FetchRequestManager<K, V> fetchRequestManager;
    private final List<Optional<? extends RequestManager>> entries;

    public RequestManagers(CoordinatorRequestManager coordinatorRequestManager,
                           CommitRequestManager commitRequestManager,
                           FetchRequestManager<K, V> fetchRequestManager) {
        this(Optional.ofNullable(coordinatorRequestManager),
                Optional.ofNullable(commitRequestManager),
                fetchRequestManager);
    }

    public RequestManagers(FetchRequestManager<K, V> fetchRequestManager) {
        this(Optional.empty(), Optional.empty(), fetchRequestManager);
    }

    public RequestManagers(Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<CommitRequestManager> commitRequestManager,
                           FetchRequestManager<K, V> fetchRequestManager) {
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.commitRequestManager = commitRequestManager;
        this.fetchRequestManager = fetchRequestManager;

        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(coordinatorRequestManager);
        list.add(commitRequestManager);
        list.add(Optional.of(fetchRequestManager));
        entries = Collections.unmodifiableList(list);
    }

    public List<Optional<? extends RequestManager>> entries() {
        return entries;
    }
}
