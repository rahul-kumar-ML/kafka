// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.common.telemetry.provider;

import static org.apache.kafka.common.telemetry.provider.Utils.buildResourceFromAllLabelsWithId;
import static org.apache.kafka.common.telemetry.provider.Utils.notEmptyString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.proto.resource.v1.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.HostProcessInfoMetricsCollector;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.telemetry.ConfluentTelemetryConfig;
import org.apache.kafka.common.telemetry.ResourceBuilderFacade;
import org.apache.kafka.common.telemetry.collector.MetricsCollector;
import org.apache.kafka.common.telemetry.emitter.Context;
import org.apache.kafka.common.utils.Time;

public class KafkaClientProvider implements Provider {

  @VisibleForTesting
  public static final String DOMAIN = "io.confluent.kafka.client";
  public static final String LABEL_CLIENT_ID = "client.id";
  public static final String ADMIN_NAMESPACE = "kafka.admin.client";
  public static final String CONSUMER_NAMESPACE = "kafka.consumer";
  public static final String PRODUCER_NAMESPACE = "kafka.producer";

  private static final List<String> DEFAULT_METRICS_INCLUDE_LIST = Collections.unmodifiableList(
      Arrays.asList(
          ConfluentTelemetryConfig.DEFAULT_SYSTEM_METRICS_INCLUDE_REGEX,
          "io.confluent.kafka.client/.*("
              + "producer/connection_count"
              + "|producer/incoming_byte_rate"
              + "|producer/incoming_byte_total"
              + "|producer/outgoing_byte_rate"
              + "|producer/outgoing_byte_total"
              + "|producer/request_size_avg"
              + "|producer/request_size_max"
              + "|producer/requests_in_flight"
              + "|producer/record_error_rate"
              + "|producer/record_error_total"
              + "|producer/record_send_rate"
              + "|producer/record_send_total"
              + "|producer/batch_size_avg"
              + "|producer/batch_size_max"
              + "|producer/record_size_avg"
              + "|producer/record_size_max"
              + "|producer_node/request_size_avg"
              + "|producer_node/request_size_max"
              + "|producer_node/incoming_byte_rate"
              + "|producer_node/incoming_byte_total"
              + "|producer_node/outgoing_byte_rate"
              + "|producer_node/outgoing_byte_total"
              + "|consumer/request_size_avg"
              + "|consumer/request_size_max"
              + "|consumer_node/request_size_avg"
              + "|consumer_node/request_size_max)"
      ));

  private Resource resource;
  private ConfluentTelemetryConfig config;

  @Override
  public synchronized void configure(Map<String, ?> configs) {
    System.out.println("[APM] configure: " + configs);
    this.config = new ConfluentTelemetryConfig(configs);
  }

  @Override
  public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
    System.out.println("[APM] - validate client metrics context. Context labels: " + metricsContext.contextLabels() + " config: " + config);
    // metric collection will be disabled for clients without a client id (e.g. transient admin clients)
    return notEmptyString(config, CommonClientConfigs.CLIENT_ID_CONFIG) &&
        validateRequiredLabels(metricsContext.contextLabels());
  }

  @Override
  public void contextChange(MetricsContext metricsContext) {
    System.out.println("[APM] - Context change. Context labels: " + metricsContext.contextLabels());
    final Map<String, String> contextLabels = metricsContext.contextLabels();
    // client resource labels are prefixed with the resource type passed
    // in by the parent process as resource.type

    // resource id is currently required in ResourceBuilderFacade:
    // inherit resource.cluster.id to match what we do for resource labels in all our parent services
    String clusterId = contextLabels.get(Utils.RESOURCE_LABEL_CLUSTER_ID);
    String resourceId = clusterId;

    final ResourceBuilderFacade resourceBuilder = buildResourceFromAllLabelsWithId(metricsContext, resourceId)
        .withNamespacedLabel(LABEL_CLIENT_ID,
                             (String) this.config.originals().get(CommonClientConfigs.CLIENT_ID_CONFIG));

    if (clusterId != null) {
      // adding cluster.id is only required to ensure the label is added in case of aliasing
      resourceBuilder.withNamespacedLabel(Utils.LABEL_CLUSTER_ID, clusterId);
    }

    this.resource = resourceBuilder.build();
  }

  @Override
  public Resource resource() {
    return this.resource;
  }

  @Override
  public String domain() {
    return DOMAIN;
  }

  private  boolean validateRequiredLabels(Map<String, String> metadata) {
    return Utils.validateRequiredResourceLabels(metadata);
  }

  @Override
  public List<MetricsCollector> extraCollectors(Context ctx) {
    return ImmutableList.of(new HostProcessInfoMetricsCollector(Time.SYSTEM));
  }

  @Override
  public List<String> metricsIncludeRegexDefault() {
    return DEFAULT_METRICS_INCLUDE_LIST;
  }
}
