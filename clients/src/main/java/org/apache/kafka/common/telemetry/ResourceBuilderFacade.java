/*
 * Copyright 2020 Confluent Inc.
 */

package org.apache.kafka.common.telemetry;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.resource.v1.ResourceOrBuilder;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper on the protobuf {@link Resource.Builder} that enforces Confluent conventions.
 */
public class ResourceBuilderFacade {

  private static final Logger log = LoggerFactory.getLogger(ResourceBuilderFacade.class);

  private static final String KEY_VERSION = "version";
  private static final String KEY_ID = "id";
  private static final String TYPE = "type";

  private static final ImmutableList<ResourceLabelProvider> LABEL_PROVIDERS = ImmutableList.of(
      new JavaRuntimeResourceLabelProvider()
  );

  private final Resource.Builder resourceBuilder;
  private final String type;

  public ResourceBuilderFacade(String type) {
    this.type = Objects.requireNonNull(type, "Resource type must not be null")
        .toLowerCase(Locale.ENGLISH);
    resourceBuilder = Resource.newBuilder();
    withLabel(TYPE, type);
    LABEL_PROVIDERS.forEach(provider -> withLabels(provider.getLabels()));
  }

  private static String prefixWithNamespace(String namespace, String labelKey) {
    return namespace + "." + labelKey;
  }

  public ResourceBuilderFacade withVersion(String version) {
    Objects.requireNonNull(version, "Version must not be null");
    return withNamespacedLabel(KEY_VERSION, version);
  }

  public ResourceBuilderFacade withId(String id) {
    Objects.requireNonNull(id, "ID must not be null");
    return withNamespacedLabel(KEY_ID, id);
  }

  /**
   * Add a label to this Resource. If the label key already exists, the new value will be ignored
   * and a warning logged.
   */
  public ResourceBuilderFacade withLabel(String key, String value) {
    if (hasAttribute(resourceBuilder, key)) {
      log.warn("Ignoring redefinition of existing telemetry label {}", key);
    } else {
      this.addAttribute(key, AnyValue.newBuilder().setStringValue(value));
    }
    return this;
  }

  public ResourceBuilderFacade withLabel(String key, List<String> values) {
    if (hasAttribute(resourceBuilder, key)) {
      log.warn("Ignoring redefinition of existing telemetry label {}", key);
    } else {
      final ArrayValue.Builder valuesBuilder = ArrayValue.newBuilder();
      values.stream().map(v -> AnyValue.newBuilder().setStringValue(v)).collect(Collectors.toList()).forEach(valuesBuilder::addValues);
      this.addAttribute(key, AnyValue.newBuilder().setArrayValue(valuesBuilder));
    }
    return this;
  }

  private void addAttribute(String key, AnyValue.Builder value) {
    final KeyValue.Builder kv = KeyValue.newBuilder()
        .setKey(key)
        .setValue(value);
    resourceBuilder.addAttributes(kv);
  }

  private static boolean hasAttribute(ResourceOrBuilder resource, String key) {
    return resource.getAttributesList().stream().anyMatch(kv -> kv.getKey().equals(key));
  }

  /**
   * Add labels to this Resource. If any label keys already exist, the new value for those labels
   * will be ignored.
   */
  public ResourceBuilderFacade withLabels(Map<String, String> labels) {
    labels.forEach(this::withLabel);
    return this;
  }

  /**
   * Add a label to this Resource, prefixed with this Resource's type.
   */
  public ResourceBuilderFacade withNamespacedLabel(String key, String value) {
    // prefix label with resource type
    return withLabel(this.type + "." + key, value);
  }

  public ResourceBuilderFacade withNamespacedLabel(String key, List<String> values) {
    // prefix label with resource type
    return withLabel(this.type + "." + key, values);
  }

  /**
   * Add labels to this Resource, prefixed with this Resource's type.
   */
  public ResourceBuilderFacade withNamespacedLabels(Map<String, String> labels) {
    labels.forEach(this::withNamespacedLabel);
    return this;
  }

  /**
   * Validate a {@link Resource} conforms to Confluent conventions.
   *
   * @param resource A resource <i>potentially not constructed using this builder</i>.
   */
  static void validate(ResourceOrBuilder resource) {
    Optional<String> type = resource.getAttributesList().stream()
        .filter(kv -> TYPE.equals(kv.getKey()) && kv.getValue().hasStringValue())
        .map(kv -> kv.getValue().getStringValue())
        .findAny();
    Preconditions.checkState(type.isPresent(), "Resource type must be set");

    String versionLabel = prefixWithNamespace(type.get(), KEY_VERSION);
    Preconditions.checkState(hasAttribute(resource, versionLabel), "Resource version must be set");

    String idLabel = prefixWithNamespace(type.get(), KEY_ID);
    Preconditions.checkState(hasAttribute(resource, idLabel), "Resource ID must be set");
  }

  public Resource build() {
    validate(resourceBuilder);
    return resourceBuilder.build();
  }

}
