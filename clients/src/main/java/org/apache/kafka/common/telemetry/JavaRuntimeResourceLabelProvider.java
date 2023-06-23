/*
 * Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.common.telemetry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Properties;

/**
 * Provides labels for Java runtime environment information.
 *
 * <ul>
 *   <li>{@code java.version}</li>
 * </ul>
 */
public class JavaRuntimeResourceLabelProvider implements ResourceLabelProvider {

  public static final String NAMESPACE = "java";
  public static final String KEY_VERSION = NAMESPACE + ".version";

  private final Properties systemProperties;

  public JavaRuntimeResourceLabelProvider() {
    this(System.getProperties());
  }

  @VisibleForTesting
  JavaRuntimeResourceLabelProvider(Properties systemProperties) {
    this.systemProperties = systemProperties;
  }

  @Override
  public Map<String, String> getLabels() {
    return ImmutableMap.of(KEY_VERSION, systemProperties.getProperty("java.version"));
  }

}
