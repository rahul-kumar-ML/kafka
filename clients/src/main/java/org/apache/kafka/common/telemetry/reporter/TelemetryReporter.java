package org.apache.kafka.common.telemetry.reporter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.ClientTelemetryEmitter;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.telemetry.ConfluentTelemetryConfig;
import org.apache.kafka.common.telemetry.MetricsCollectorTask;
import org.apache.kafka.common.telemetry.collector.KafkaMetricsCollector;
import org.apache.kafka.common.telemetry.collector.MetricsCollector;
import org.apache.kafka.common.telemetry.emitter.Context;
import org.apache.kafka.common.telemetry.emitter.Emitter;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;
import org.apache.kafka.common.telemetry.metrics.MetricNamingStrategy;
import org.apache.kafka.common.telemetry.provider.Provider;
import org.apache.kafka.common.telemetry.provider.ProviderRegistry;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryReporter implements MetricsReporter {

  static final MetricNamingStrategy<MetricName> NAMING_STRATEGY = new MetricNamingStrategy<MetricName>() {

    @Override
    public MetricKey metricKey(MetricName metricName) {
      return new MetricKey(metricName);
    }

    @Override
    public MetricKey derivedMetricKey(MetricKey key, String derivedComponent) {
      return new MetricKey(key.name() + '/' + derivedComponent, key.tags());
    }

  };

//  static final Predicate<KafkaMetric> KAFKA_METRIC_PREDICATE = m -> m.metricName().name().startsWith("org.apache.kafka");

  static final Predicate<KafkaMetric> KAFKA_METRIC_PREDICATE = m -> true;

  public final static Predicate<? super MetricKeyable> SELECTOR_ALL_METRICS = k -> true;

  private static final Logger log = LoggerFactory.getLogger(TelemetryReporter.class);
  public static final String SELF_METRICS_NAMESPACE = "confluent.telemetry";

  public static final String TELEMETRY_REPORTER_ID_TAG = "telemetry-reporter-id";

  /**
   * Note that rawOriginalConfig may be different than originalConfig.originals()
   * if we're on the broker (since we inject local exporter configs before creating
   * the ConfluentTelemetryConfig object).
   */
  private Map<String, Object> rawOriginalConfig;
  private ConfluentTelemetryConfig originalConfig;
  private ConfluentTelemetryConfig config;
  private volatile Context ctx;

  private MetricsCollectorTask collectorTask;
  private final List<MetricsCollector> collectors = new CopyOnWriteArrayList<>();
  private volatile KafkaMetricsCollector kafkaMetricsCollector;
  private volatile ClientTelemetryEmitter emitter;

  private Provider activeProvider;

  private Metrics selfMetrics;
  private Set<String> activeFilters = Collections.singleton(ConfluentTelemetryConfig.DEFAULT_NAMED_FILTER_NAME);

  private final Uuid reporterId = Uuid.randomUuid();

  public TelemetryReporter() {
  }

  /**
   * Note: we are assuming that these methods are invoked in the following order:
   *  1. configure() [must be called first & only once]
   *  2. contextChange() [must be called second]
   *  3. contextChange() / reconfigurableConfigs() / reconfigure() [each may be called multiple times]
   */
  @SuppressWarnings("unchecked")
  @Override
  public synchronized void configure(Map<String, ?> configs) {
    rawOriginalConfig = (Map<String, Object>) configs;
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    createConfiguration(configs, false);
  }

  /* Implementing Reconfigurable interface to make this reporter dynamically reconfigurable. */
  @Override
  public synchronized void reconfigure(Map<String, ?> newRawConfig) {
    Preconditions.checkState(config != null && emitter != null,
        "contextChange() was not called before reconfigure()");

    ConfluentTelemetryConfig oldConfig = config;
    config = createConfiguration(newRawConfig, true);
    // As the collector.include is reconfigurable and defines the _default filter
    // we need to update the metricFilters keeping any filter already defined
  }

  private ConfluentTelemetryConfig createConfiguration(Map<String, ?> configs, boolean doLog) {
    // the original config may contain local exporter overrides so add those first
    Map<String, Object> validateConfig = Maps.newHashMap(originalConfig.originals());

    // put all filtered configs (avoid applying configs that are not dynamic)
    // TODO: remove once this is fixed https://confluentinc.atlassian.net/browse/CPKAFKA-4828
    validateConfig.putAll(onlyReconfigurables(configs));

    // validation should be handled by ConfigDef Validators
    return new ConfluentTelemetryConfig(validateConfig, doLog);
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    if (config == null) {
      throw new IllegalStateException("contextChange() was not called before reconfigurableConfigs()");
    }
    Set<String> reconfigurables = new HashSet<>(ConfluentTelemetryConfig.RECONFIGURABLES);

    return reconfigurables;
  }

  @Override
  public synchronized void contextChange(MetricsContext metricsContext) {
    /**
     * Select the provider on {@link MetricsReporter#contextChange(MetricsContext)}.
     *
     * 1. Lookup the provider from the {@link ProviderRegistry} using _namespace tag in the
     * MetricsContext metadata.
     * 2. If a provider is found, validated all required labels are available
     * 3. If validation succeeds: initialize the provider, start the metric collection task, set metrics labels for services/libraries that expose metrics
     */
    Preconditions.checkState(this.rawOriginalConfig != null, "configure() was not called before contextChange()");

    log.info("metricsContext {}", metricsContext.contextLabels());
    if (!metricsContext.contextLabels().containsKey(MetricsContext.NAMESPACE)) {
      log.error("_namespace not found in metrics context. Metrics collection is disabled");
      return;
    }

    collectors.forEach(MetricsCollector::stop);

    activeProvider = ProviderRegistry.getProvider(metricsContext.contextLabels().get(MetricsContext.NAMESPACE));

    if (activeProvider == null) {
      log.error("No provider was detected for context {}. Available providers {}.",
          metricsContext.contextLabels(),
          ProviderRegistry.providers.keySet());
      return;
    }

    log.info("provider {} is selected.", activeProvider.getClass().getCanonicalName());

    if (!activeProvider.validate(metricsContext, rawOriginalConfig)) {
      log.warn("Validation failed for {} context {}", activeProvider.getClass(), metricsContext.contextLabels());
     return;
    }

    if (collectorTask == null) {
      log.info("collector task is null");
      // Initialize the provider only once. contextChange(..) can be called more than once,
      //but once it's been initialized and all necessary labels are present then we don't re-initialize again.
      activeProvider.configure(rawOriginalConfig);
    }

    activeProvider.contextChange(metricsContext);

    if (collectorTask == null) {
      log.info("collector task is null1");
      // we need to wait for contextChange call to initialize configs (due to 'isBroker' check)
      initConfig();
      initContext();
      initSelfMetrics();
      initCollectors();
      // emitter needs to be created after the collectors and exporters
//      createEmitter();
//      startMetricCollectorTask();
    }
  }

  private void initConfig() {
    originalConfig = new ConfluentTelemetryConfig(rawOriginalConfig);
    config = originalConfig;
  }

  private void initContext() {
    Resource.Builder builder = activeProvider.resource().toBuilder();
    //add labels defined in properties file to resource: confluent.telemetry.labels.*
    config.getLabels().forEach((k, v) -> builder.addAttributesBuilder()
        .setKey(k)
        .setValue(AnyValue.newBuilder().setStringValue(v)));
    Resource resource = builder.build();
    ctx = new Context(
        resource,
        activeProvider.domain()
    );
  }

  private void startMetricCollectorTask() {
    long collectIntervalMs = config.getLong(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG);
    log.info("Starting Confluent telemetry reporter with an interval of {} ms)", collectIntervalMs);
    log.info("Telemetry reporter resource attributes: {}", ctx.getResource().getAttributesList());
    collectorTask = new MetricsCollectorTask(
      collectors,
      collectIntervalMs,
      emitter
    );

    collectors.forEach(MetricsCollector::start);

    collectorTask.start();
  }

  private void initSelfMetrics() {

    KafkaMetricsCollector selfMetricsCollector = new KafkaMetricsCollector(
        Time.SYSTEM, NAMING_STRATEGY, KAFKA_METRIC_PREDICATE
    );
    collectors.add(selfMetricsCollector);
    MetricConfig config = new MetricConfig().tags(ImmutableMap.of(
        TELEMETRY_REPORTER_ID_TAG, reporterId.toString()
    ));
    selfMetrics = new Metrics(
        config,
        ImmutableList.of(
            // expose self-metrics via JMX
            new JmxReporter(),
            // shim to expose self-metrics with telemetry-reporter itself
            new MetricsReporter() {
              @Override
              public void init(List<KafkaMetric> metrics) {
                selfMetricsCollector.init(metrics);
              }

              @Override
              public void metricChange(KafkaMetric metric) {
                selfMetricsCollector.metricChange(metric);
              }

              @Override
              public void metricRemoval(KafkaMetric metric) {
                selfMetricsCollector.metricRemoval(metric);
              }

              @Override
              public void close() {
                // selfMetricsCollector does not need to be closed
              }

              @Override
              public void configure(Map<String, ?> configs) {
                // selfMetricsCollector is already configured based on the telemetry reporter config
              }
            }),
        Time.SYSTEM,
        new KafkaMetricsContext(SELF_METRICS_NAMESPACE));
  }

  private void initCollectors() {
    kafkaMetricsCollector = new KafkaMetricsCollector(
        Time.SYSTEM, NAMING_STRATEGY, KAFKA_METRIC_PREDICATE
    );
    collectors.add(kafkaMetricsCollector);
    collectors.addAll(activeProvider.extraCollectors(ctx));
  }

  public List<MetricsCollector> getCollectors() {
    return collectors;
  }

  /**
   * Called when the metrics repository is closed.
   */
  @Override
  public void close() {
    log.info("Stopping TelemetryReporter collectorTask");
    if (collectorTask != null) {
        collectorTask.close();
    }

    collectors.forEach(MetricsCollector::stop);

    if (selfMetrics != null) {
      selfMetrics.close();
    }
  }

  @Override
  public void init(List<KafkaMetric> metrics) {
    // metrics collector may not have been initialized (e.g. invalid context labels)
    // in which case metrics collection is disabled
    if (kafkaMetricsCollector != null) {
      kafkaMetricsCollector.init(metrics);
    }
  }

  /**
   * This is called whenever a metric is added/registered
   */
  @Override
  public void metricChange(KafkaMetric metric) {
    // metrics collector may not have been initialized (e.g. invalid context labels)
    // in which case metrics collection is disabled
    if (kafkaMetricsCollector != null) {
      kafkaMetricsCollector.metricChange(metric);
    }
  }

  /**
   * This is called whenever a metric is removed
   */
  @Override
  public void metricRemoval(KafkaMetric metric) {
    // metrics collector may not have been initialized (e.g. invalid context labels)
    // in which case metrics collection is disabled
    if (kafkaMetricsCollector != null) {
      kafkaMetricsCollector.metricRemoval(metric);
    }
  }

  private Map<String, ?> onlyReconfigurables(Map<String, ?> originals) {
    return reconfigurableConfigs().stream()
      // It is possible to be passed 'null' values through our dynamic config mechanism.
      // We should strip 'null' values since it's not useful for us to know that a config is not set.
      // Without stripping 'null' values we will hit https://bugs.openjdk.java.net/browse/JDK-8148463
      .filter(c -> originals.get(c) != null)
      .collect(Collectors.toMap(c -> c, c -> originals.get(c)));
  }

  public Emitter emitter() {
    if (emitter == null) {
      throw new IllegalStateException("emitter() was called before the Emitter was instantiated.");
    }
    return emitter;
  }

  private void createEmitter() {
    emitter = new ClientTelemetryEmitter(SELECTOR_ALL_METRICS);
  }

  public Resource resource() {
    return activeProvider.resource();
  }
}
