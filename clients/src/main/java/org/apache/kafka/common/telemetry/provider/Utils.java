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
package org.apache.kafka.common.telemetry.provider;

import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.telemetry.collector.MetricsCollector;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricNamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  /**
   * Validate that the map contains the key and the key is a non-empty string
   *
   * @return true if key is valid.
   */
  public static boolean notEmptyString(Map<String, ?> m, String key) {
    if (!m.containsKey(key)) {
      log.trace("{} does not exist in map {}", key, m);
      return false;
    }

    if (m.get(key) == null) {
      log.trace("{} is null. map {}", key, m);
      return false;
    }

    if (!(m.get(key) instanceof String)) {
      log.trace("{} is not a string. map {}", key, m);
      return false;
    }

    String val = (String) m.get(key);

    if (val.isEmpty()) {
      log.trace("{} is empty string. value = {} map {}", key, val, m);
      return false;
    }
    return true;
  }

  public static boolean validateRequiredResourceLabels(Map<String, String> metadata) {
    return notEmptyString(metadata, MetricsContext.NAMESPACE);
  }

}
