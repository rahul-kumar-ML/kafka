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

package org.apache.kafka.clients.telemetry.collector;

import org.apache.kafka.clients.telemetry.emitter.Emitter;

// A collector is responsible for scraping a source of metrics and converting them to the canonical format
// For eg: we will have collectors for system metrics, kafka metrics, yammer metrics, opencensus metric ....
public interface MetricsCollector {

    /**
     * Label for the collector that collected the metrics
     */
    String LABEL_COLLECTOR = "collector";

    /**
     * Label for the metric instrumentation library
     */
    String LABEL_LIBRARY = "library";

    /**
     * "none" value for {@link #LABEL_LIBRARY}
     */
    String LIBRARY_NONE = "none";

    /**
     * Label for the original (untranslated) metric name
     */
    String LABEL_ORIGINAL = "metric_name_original";

    void collect(Emitter emitter);

    default void start() { }

    default void stop() { }
}
