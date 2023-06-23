/*
 * Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.common.telemetry;

import java.util.Map;

/**
 * Provides labels from the runtime context (Cloud, Deployment, Host, JVM, etc) that can be set on
 * the {@link io.opencensus.proto.resource.v1.Resource}.
 */
public interface ResourceLabelProvider {

  /**
   * Get labels to be set on the {@link io.opencensus.proto.resource.v1.Resource}. The labels should
   * be namespaced according to the conventions defined <a
   * href="https://github.com/confluentinc/schroedinger/blob/master/telemetry-api/docs/conventions.md">here</a>.
   *
   * @return A (possibly empty) map of label-value pairs.
   */
  Map<String, String> getLabels();

}
