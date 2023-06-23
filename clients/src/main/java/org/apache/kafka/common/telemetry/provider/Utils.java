package org.apache.kafka.common.telemetry.provider;

import static org.apache.kafka.common.telemetry.provider.Provider.EXCLUDE_ALL;

import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.telemetry.ResourceBuilderFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  public static final String RESOURCE_LABEL_PREFIX = "resource.";
  public static final String RESOURCE_LABEL_TYPE = RESOURCE_LABEL_PREFIX + "type";
  public static final String RESOURCE_LABEL_VERSION = RESOURCE_LABEL_PREFIX + "version";
  public static final String RESOURCE_LABEL_COMMIT_ID = RESOURCE_LABEL_PREFIX + "commit.id";

  public static final String LABEL_CLUSTER_ID = "cluster.id";
  public static final String RESOURCE_LABEL_CLUSTER_ID = RESOURCE_LABEL_PREFIX + LABEL_CLUSTER_ID;

  // Kafka-specific labels
  public static final String KAFKA_BROKER_ID = "kafka.broker.id";
  public static final String KAFKA_NODE_ID = "kafka.node.id";
  public static final String KAFKA_CLUSTER_ID = "kafka.cluster.id";
  public static final String KAFKA_CELL_ID = "kafka.cell.id";
  public static final String KAFKA_PROCESS_ROLES = "kafka.process.roles";

  // https://confluentinc.atlassian.net/browse/OBSTEL-227
  public static final String KAFKA_CONTROLLER_ROLE = "controller";
  public static final String KAFKA_BROKER_ROLE = "broker";

  // Connect-specific labels
  public static final String CONNECT_KAFKA_CLUSTER_ID = "connect.kafka.cluster.id";
  public static final String CONNECT_GROUP_ID = "connect.group.id";
  private static final Set<String> EXCLUDED_LABELS = ImmutableSet.of(MetricsContext.NAMESPACE, KAFKA_CLUSTER_ID, KAFKA_BROKER_ID,
          KAFKA_CELL_ID, KAFKA_PROCESS_ROLES, CONNECT_KAFKA_CLUSTER_ID, CONNECT_GROUP_ID, RESOURCE_LABEL_TYPE);

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

  /**
   * Extract the resource labels from the metrics context metadata and remove metrics context
   * prefix.
   *
   * @return a map of cleaned up labels.
   */
  public static Map<String, String> getResourceLabels(Map<String, String> metricsCtxMetadata) {
    return metricsCtxMetadata.entrySet().stream()
        .filter(e -> !EXCLUDED_LABELS.contains(e.getKey()))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey()
                    .replace(RESOURCE_LABEL_PREFIX, ""),
                entry -> entry.getValue())
        );
  }

  public static Map<String, String> getNonResourceLabels(Map<String, String> metricsCtxMetadata) {
    Set<String> exclude = ImmutableSet.of(MetricsContext.NAMESPACE);
    return metricsCtxMetadata.entrySet().stream()
        .filter(e -> !exclude.contains(e.getKey()) && !e.getKey().startsWith(RESOURCE_LABEL_PREFIX))
        .collect(Collectors.toMap(
            entry -> entry.getKey(),
            entry -> entry.getValue())
        );
  }

  public static ResourceBuilderFacade buildResourceFromAllLabelsWithId(MetricsContext metricsContext, String id) {
    String type = metricsContext.contextLabels().get(RESOURCE_LABEL_TYPE);
    String version = metricsContext.contextLabels()
        .get(RESOURCE_LABEL_VERSION);
    ResourceBuilderFacade resourceBuilderFacade = new ResourceBuilderFacade(type.toLowerCase(Locale.ROOT))
        .withVersion(version)
        .withId(id)
        // RESOURCE LABELS (prefixed with resource type)
        .withNamespacedLabels(getResourceLabels(metricsContext.contextLabels()))
        // Non RESOURCE LABELS, this will add apache kafka labels: connect.kafka.cluster.id, connect.group.id
        .withLabels(getNonResourceLabels(metricsContext.contextLabels()));

    return resourceBuilderFacade;
  }

  public static boolean validateRequiredResourceLabels(Map<String, String> metadata) {
    return notEmptyString(metadata, MetricsContext.NAMESPACE);
  }

  public static Predicate<String> configPredicate(String regexString) {
    regexString = regexString.trim();

    if (regexString.isEmpty()) {
      return EXCLUDE_ALL;
    }
    // sourceCompatibility and targetCompatibility is set to Java 8 in build.gradle hence avoid
    // using `asMatchPredicate` method of `Predicate` class.
    Pattern pattern = Pattern.compile(regexString);

    return configName -> pattern.matcher(configName).matches();
  }

}
