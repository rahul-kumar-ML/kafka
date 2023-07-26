// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.common.telemetry.provider;

import com.google.common.base.Strings;
import com.yammer.metrics.core.MetricName;
import io.opentelemetry.proto.resource.v1.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.metrics.KafkaYammerMetrics;
import org.apache.kafka.common.telemetry.collector.MetricsCollector;
import org.apache.kafka.common.telemetry.collector.YammerMetricsCollector;
import org.apache.kafka.common.telemetry.emitter.Context;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricNamingStrategy;

/**
 * Implement this interface to collect metrics for your component. You will need to register your
 * implementation in {@link ProviderRegistry}.
 */
public interface Provider extends Configurable {

  String NAME_JOINER = "/";

  MetricNamingStrategy<com.yammer.metrics.core.MetricName> NAMING_STRATEGY = new MetricNamingStrategy<com.yammer.metrics.core.MetricName>() {

    @Override
    public MetricKey metricKey(com.yammer.metrics.core.MetricName metricName) {
//      String name = fullMetricName("test", metricName.getType(), metricName.getName());
      String mbeanName = Strings.nullToEmpty(metricName.getMBeanName());
      Map<String, String> labels = new HashMap<>();
      return new MetricKey(metricName.getName(), labels);
    }

    @Override
    public MetricKey derivedMetricKey(MetricKey key, String derivedComponent) {
      return new MetricKey(key.name() + NAME_JOINER + derivedComponent, key.tags());
    }

  };

  static String fullMetricName(String domain, String group, String name) {
    return domain
        + NAME_JOINER
        + group
        + NAME_JOINER
        + name;
  }

  Predicate<String> EXCLUDE_ALL = k -> false;

  /**
   * Validate that all the data required for generating correct metrics is present. The provider
   * will be disabled if validation fails.
   *
   * @param metricsContext {@link MetricsContext}
   * @return false if all the data required for generating correct metrics is missing, true
   * otherwise.
   */
  boolean validate(MetricsContext metricsContext, Map<String, ?> config);

  /**
   * Domain of the active provider. This is used by other parts of the reporter.
   *
   * @return Domain in string format.
   */
  String domain();

  /**
   * The resource for this provider.
   *
   * @return A fully formed {@link Resource} will all the tags.
   */
  Resource resource();

  /**
   * Sets the metrics labels for the service or library exposing metrics. This will be called before {@link org.apache.kafka.common.metrics.MetricsReporter#init(List)} and may be called anytime after that.
   *
   * @param metricsContext {@link MetricsContext}
   */
  void contextChange(MetricsContext metricsContext);

  /**
   * The metrics include regular expression list used as a default value for this provider.
   * This list will be joined as following '.*<EXPR>.*|.*<EXPR>.*' to form a union.
   *
   * @return a list of regular expressions to match metric names
   */
  List<String> metricsIncludeRegexDefault();

  /**
   * The collector for Kafka Metrics library is enabled by default. If you need any more, add them
   * here.
   *
   * @param ctx {@link Context}
   * @return List of extra collectors
   */
  default List<MetricsCollector> extraCollectors(Context ctx) {
    return Collections.emptyList();
  }

  /**
   * Include list for config
   * @return Regex for filtering config keys.
   */
  default Predicate<String> configInclude() {
    // exclude everything by default
    return EXCLUDE_ALL;
  }
}
