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

package org.apache.kafka.clients.telemetry.reporter;

import org.apache.kafka.clients.telemetry.Context;
import org.apache.kafka.clients.telemetry.collector.MetricsCollector;
import org.apache.kafka.clients.telemetry.emitter.TelemetryEmitter;
import org.apache.kafka.clients.telemetry.exporter.Exporter;
import org.apache.kafka.clients.telemetry.metrics.Keyed;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Provider;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TelemetryReporter implements MetricsReporter, ClusterResourceListener {

  private static final Logger log = LoggerFactory.getLogger(TelemetryReporter.class);
  public static final String SELF_METRICS_DOMAIN = "io.confluent.telemetry";
  public static final String SELF_METRICS_NAMESPACE = "confluent.telemetry";

  /**
   * Note that rawOriginalConfig may be different than originalConfig.originals()
   * if we're on the broker (since we inject local exporter configs before creating
   * the ConfluentTelemetryConfig object).
   */
  private Map<String, Object> rawOriginalConfig;
  private volatile Context ctx;

  private MetricsCollectorTask collectorTask;
  private final Map<String, Exporter> exporters = new ConcurrentHashMap<>();
  private final List<MetricsCollector> collectors = new CopyOnWriteArrayList<>();
  // private volatile KafkaMetricsCollector kafkaMetricsCollector;
  private volatile TelemetryEmitter emitter;
  private volatile Predicate<Keyed> unionPredicate;

  private Provider activeProvider;

  private Metrics selfMetrics;


  public TelemetryReporter() {
    this(
        config -> {
          EventLogger eventLogger = new EventLogger();
          eventLogger.configure(config);
          return eventLogger;
        }
    );
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
    this.rawOriginalConfig = (Map<String, Object>) configs;
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    createConfiguration(configs, false);
  }

  /* Implementing Reconfigurable interface to make this reporter dynamically reconfigurable. */
  @Override
  public synchronized void reconfigure(Map<String, ?> configs) {
/*
    reconfigureExporters(oldConfig, newConfig);
    reconfigureEventEmitter(configs);
    */
  }

  private void initExporters() {
    initExporters(
      this.config.enabledExporters()
    );
  }

  private void initExporters(
        Map<String, ExporterConfig> toInit
  ) {
    for (Map.Entry<String, ExporterConfig> entry : toInit.entrySet()) {
      ExporterConfig exporterConfig = entry.getValue();
      String exporterName = entry.getKey();
      log.info("Creating {} exporter named '{}'", exporterConfig.getType().name(), exporterName);

      Exporter newExporter = null;

      if (exporterConfig instanceof KafkaExporterConfig) {
        newExporter = KafkaExporter.newBuilder((KafkaExporterConfig) exporterConfig).setName(exporterName).build();
      } else if (exporterConfig instanceof HttpExporterConfig) {
        newExporter = new HttpExporter(exporterName, (HttpExporterConfig) exporterConfig);
      } else {
        throw new IllegalStateException("Unexpected exporter config: " + exporterConfig.getClass());
      }

      newExporter.setMetricsRegistry(selfMetrics);

      this.exporters.put(exporterName, newExporter);
    }
  }

  private void updateExporters(
          Map<String, ExporterConfig> toReconfigure
  ) {
    // reconfigure exporters
    for (Map.Entry<String, ExporterConfig> entry : toReconfigure.entrySet()) {
      Exporter exporter = this.exporters.get(entry.getKey());
      ExporterConfig exporterConfig = entry.getValue();
      if (exporter instanceof HttpExporter) {
        ((HttpExporter) exporter).reconfigure((HttpExporterConfig) exporterConfig);
      } else if (exporter instanceof KafkaExporter) {
        ((KafkaExporter) exporter).reconfigure((KafkaExporterConfig) exporterConfig);
      }
    }
  }

  private void closeExporters(
          Map<String, ExporterConfig> toClose
  ) {
    // shutdown exporters
    for (Map.Entry<String, ExporterConfig> entry : toClose.entrySet()) {
      log.info("Closing {} exporter named '{}'", entry.getValue().getType().name(), entry.getKey());
      Exporter exporter = this.exporters.remove(entry.getKey());

      try {
        exporter.close();
      } catch (Exception e) {
        log.warn("exception closing {} exporter named '{}'",
                entry.getValue().getType(), entry.getKey(), e
        );
      }
    }
  }

  private void reconfigureExporters(ConfluentTelemetryConfig oldConfig, ConfluentTelemetryConfig newConfig) {
    Set<String> oldEnabled = oldConfig.enabledExporters().keySet();
    Set<String> newEnabled = newConfig.enabledExporters().keySet();
    closeExporters(
      newConfig.allExportersWithNames(
        Sets.difference(oldEnabled, newEnabled)
      )
    );
    updateExporters(
      newConfig.allExportersWithNames(
        Sets.intersection(oldEnabled, newEnabled)
      )
    );
    initExporters(
      newConfig.allExportersWithNames(
        Sets.difference(newEnabled, oldEnabled)
      )
    );
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    if (this.config == null) {
      throw new IllegalStateException("contextChange() was not called before reconfigurableConfigs()");
    }
    Set<String> reconfigurables = new HashSet<String>(ConfluentTelemetryConfig.RECONFIGURABLES);

    // handle generic exporter configs
    for (String name : this.config.allExporters().keySet()) {
      reconfigurables.addAll(
        ExporterConfig.RECONFIGURABLES.stream()
          .map(c -> ConfluentTelemetryConfig.exporterPrefixForName(name) + c)
          .collect(Collectors.toSet())
      );
    }

    // HttpExporterConfig related reconfigurable configs.
    for (String name : this.config.allHttpExporters().keySet()) {
      reconfigurables.addAll(
        HttpExporterConfig.RECONFIGURABLE_CONFIGS.stream()
          .map(c -> ConfluentTelemetryConfig.exporterPrefixForName(name) + c)
          .collect(Collectors.toSet())
        );
    }

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
    if (this.rawOriginalConfig == null) {
      throw new IllegalStateException("configure() was not called before contextChange()");
    }

    log.debug("metricsContext {}", metricsContext.contextLabels());
    if (!metricsContext.contextLabels().containsKey(MetricsContext.NAMESPACE)) {
      log.error("_namespace not found in metrics context. Metrics collection is disabled");
      return;
    }

    collectors.forEach(MetricsCollector::stop);

    this.activeProvider = ProviderRegistry.getProvider(metricsContext.contextLabels().get(MetricsContext.NAMESPACE));

    if (this.activeProvider == null) {
      log.error("No provider was detected for context {}. Available providers {}.",
          metricsContext.contextLabels(),
          ProviderRegistry.providers.keySet());
      return;
    }

    log.debug("provider {} is selected.", this.activeProvider.getClass().getCanonicalName());

    if (!this.activeProvider.validate(metricsContext, this.rawOriginalConfig)) {
      log.warn("Validation failed for {} context {}", this.activeProvider.getClass(), metricsContext.contextLabels());
     return;
    }

    if (this.collectorTask == null) {
      // Initialize the provider only once. contextChange(..) can be called more than once,
      //but once it's been initialized and all necessary labels are present then we don't re-initialize again.
      this.activeProvider.configure(this.rawOriginalConfig);
    }

    this.activeProvider.contextChange(metricsContext);

    if (this.collectorTask == null) {

      // we need to wait for contextChange call to initialize configs (due to 'isBroker' check)
      initConfig();
      startMetricCollectorTask();
    }

    Event configEvent = configEvent(config.originals(),
            activeProvider.configInclude(),
            ctx.getResource(),
            activeProvider,
            activeProvider.domain() + "/config/static");

    eventEmitterOpt.ifPresent(
            eventEmitter -> {
              eventEmitter.setEventLabels(activeProvider.resource().getLabelsMap());
              eventEmitter.emit(configEvent);
            });
    if (this.configEventLogger != null) {
      this.configEventLogger.log(configEvent);
    }
  }

  private void initConfig() {
    this.originalConfig = new ConfluentTelemetryConfig(
        injectProviderConfigs(this.activeProvider, this.rawOriginalConfig));
    maybeInitEventLogger(originalConfig);
    this.config = originalConfig;
    this.unionPredicate = createUnionPredicate(this.config);
  }

  private Map<String, Object> toEventLoggerConfig(ConfluentTelemetryConfig config) {
    // Inherit http exporter configs from the _confluent http exporter.
    ExporterConfig defaultHttpExporterConfig = config.allExporters().get(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME);
    if (defaultHttpExporterConfig == null) {
      // we apply this as a default so this should never happen
      throw new ConfigException(
          "Expected exporter '" + ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME
          + "' to exist but it does not."
      );
    }

    // originals() will actually contain our defaults as well as user-provided configs
    Map<String, Object> eventConfig = defaultHttpExporterConfig.originals();

    // Add required configs for event logger.
    eventConfig.put(EventLoggerConfig.EVENT_EXPORTER_CLASS_CONFIG, EventHttpExporter.class.getCanonicalName());
    return eventConfig;
  }

  private void maybeInitEventLogger(ConfluentTelemetryConfig config) {
    if (config.getBoolean(CONFIG_EVENTS_ENABLE_CONFIG)) {
      initEventLogger(config);
    }
  }

  private void reconfigureEventLogger(ConfluentTelemetryConfig oldConfig, ConfluentTelemetryConfig newConfig) {
    boolean eventLoggerWasEnabled = oldConfig.getBoolean(CONFIG_EVENTS_ENABLE_CONFIG);
    boolean eventLoggerIsEnabled = newConfig.getBoolean(CONFIG_EVENTS_ENABLE_CONFIG);
    if (!eventLoggerIsEnabled) {
      closeEventLogger();
    } else if (eventLoggerWasEnabled) {
      reconfigureEventLogger(this.config);
    } else {
      initEventLogger(this.config);
    }
  }

  private void initEventLogger(ConfluentTelemetryConfig config) {
    if (this.configEventLogger != null) {
      log.warn(
          "Trying to initialize the event logger but it's already initialized! "
          + "Will not initialize another one..."
      );
      return;
    }
    log.info("Initializing the event logger");
    this.configEventLogger = eventLoggerFactory.create(toEventLoggerConfig(config));
  }

  private void reconfigureEventLogger(ConfluentTelemetryConfig config) {
    if (this.configEventLogger == null) {
      log.warn("Trying to reconfigure the event logger but it's not initialized!");
      return;
    }
    configEventLogger.reconfigure(toEventLoggerConfig(config));
  }

  private void closeEventLogger() {
    if (this.configEventLogger == null) {
      return;
    }
    log.info("Closing the event logger");
    try {
      configEventLogger.close();
      configEventLogger = null;
    } catch (Exception e) {
      log.warn("Exception closing event logger", e);
    }
  }

  private void startMetricCollectorTask() {
    Resource.Builder builder = this.activeProvider.resource().toBuilder();
    //add labels defined in properties file to resource: confluent.telemetry.labels.*
    config.getLabels().forEach((k, v) -> builder.putLabels(k, v));
    Resource resource = builder.build();
    ctx = new Context(
      resource,
      this.activeProvider.domain(),
      config.getBoolean(ConfluentTelemetryConfig.DEBUG_ENABLED)
    );

    initSelfMetrics();
    initCollectors();
    initExporters();

    long collectIntervalMs = config.getLong(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG);
    log.info("Starting Confluent telemetry reporter with an interval of {} ms for resource = (type = {})",
        collectIntervalMs,
        ctx.getResource().getType());
    log.debug("Telemetry reporter resource labels: {}", ctx.getResource().getLabelsMap());
    this.collectorTask = new MetricsCollectorTask(
      collectors,
      collectIntervalMs,
      createEmitter()
    );

    collectors.forEach(MetricsCollector::start);

    this.collectorTask.start();
  }

  private void initSelfMetrics() {

    KafkaMetricsCollector selfMetricsCollector = new KafkaMetricsCollector(
        ConfluentMetricNamingConvention.forKafkaMetrics(SELF_METRICS_DOMAIN, ctx.isDebugEnabled(), ctx.isDebugEnabled())
    );
    collectors.add(selfMetricsCollector);
    selfMetrics = new Metrics(
        new MetricConfig(),
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
        ConfluentMetricNamingConvention.forKafkaMetrics(ctx.getDomain(), ctx.isDebugEnabled(), ctx.isDebugEnabled())
    );
    collectors.add(kafkaMetricsCollector);
    collectors.addAll(this.activeProvider.extraCollectors(ctx));
  }

  @VisibleForTesting
  Map<String, Exporter> getExporters() {
    return this.exporters;
  }

  @VisibleForTesting
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

    closeEventLogger();

    for (Exporter exporter : exporters.values()) {
      try {
        exporter.close();
      } catch (Exception e) {
        log.error("Error while closing {}", exporter, e);
      }
    }

    this.closeEventEmitter();
  }

  @Override
  public synchronized void onUpdate(ClusterResource clusterResource) {
    // NOOP. The cluster id is part of metrics context.
  }

  @Override
  public void init(List<KafkaMetric> metrics) {
    if (metrics.isEmpty()) {
      return;
    }
    if (kafkaMetricsCollector == null) {
      throw new IllegalStateException("init called before contextChange");
    }
    kafkaMetricsCollector.init(metrics);
  }

  /**
   * This is called whenever a metric is added/registered
   */
  @Override
  public void metricChange(KafkaMetric metric) {
    if (kafkaMetricsCollector == null) {
      throw new IllegalStateException("metricChange called before contextChange");
    }
    kafkaMetricsCollector.metricChange(metric);
  }

  /**
   * This is called whenever a metric is removed
   */
  @Override
  public void metricRemoval(KafkaMetric metric) {
    kafkaMetricsCollector.metricRemoval(metric);
  }

  private Map<String, ?> onlyReconfigurables(Map<String, ?> originals) {
    return reconfigurableConfigs().stream()
      // It is possible to be passed 'null' values through our dynamic config mechanism.
      // We should strip 'null' values since it's not useful for us to know that a config is not set.
      // Without stripping 'null' values we will hit https://bugs.openjdk.java.net/browse/JDK-8148463
      .filter(c -> originals.get(c) != null)
      .collect(Collectors.toMap(c -> c, c -> originals.get(c)));
  }

  private static Predicate<Keyed> createUnionPredicate(ConfluentTelemetryConfig config) {
    List<Predicate<Keyed>> enabledPredicates =
        config.enabledExporters()
            .values().stream()
            .map(ExporterConfig::buildMetricsPredicate)
            .collect(Collectors.toList());

    // combine all the predicates with ORs
    return
        enabledPredicates
            .stream()
            .reduce(Predicate::or)
            // if there are no exporters, then never match metrics
            .orElse(metricKey -> false);
  }

  private static Map<String, Object> prefixedExporterConfigs(String prefix, Map<String, Object> configs) {
    String exporterPrefix = ConfluentTelemetryConfig.exporterPrefixForName(prefix);
    return configs.entrySet().stream()
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toMap(
            e -> exporterPrefix + e.getKey(),
            e -> e.getValue()));
  }

  // visible for testing
  protected static Map<String, Object> injectProviderConfigs(Provider provider, Map<String, Object> originals) {
    return maybeInjectLocalExporter(provider,
        maybeInjectProviderDefaultIncludeConfig(provider, originals));
  }

  private static Map<String, Object> maybeInjectProviderDefaultIncludeConfig(
      Provider provider, Map<String, Object> originals) {
    Map<String, Object> configs = new HashMap<>(originals);
    // The aliasing of confluent.telemetry.metrics.collector.whitelist to
    // confluent.telemetry.metrics.collector.include happens inside ConfluentTelemetryConfig init.
    // Hence at this point, we shouldn't override collector.include with the Provider's default
    // include list when either whitelist or include list config is present in the originals.
    if (!originals.containsKey(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG)
        && !originals.containsKey(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG_ALIAS)) {
      String selfMetricsIncludeRegex = SELF_METRICS_DOMAIN + "/.*";
      configs.put(
          ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG,
          ConfluentTelemetryConfig.joinIncludeRegexList(
              ImmutableList.<String>builder()
                  .add(selfMetricsIncludeRegex) // Always include self metrics regardless of the provider
                  .addAll(provider.metricsIncludeRegexDefault())
                  .build()
          )
      );
    }
    return configs;
  }

  private static Map<String, Object> maybeInjectLocalExporter(Provider provider, Map<String, Object> originals) {
    Map<String, Object> configs = new HashMap<>();
    // this check is how we determine if we're inside the broker
    if (provider instanceof KafkaServerProvider) {
      // first add the local exporter default values
      configs.putAll(
          prefixedExporterConfigs(
              ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME,
              ConfluentTelemetryConfig.EXPORTER_LOCAL_DEFAULTS)
      );

      // then apply derived broker config
      configs.putAll(
          prefixedExporterConfigs(
              ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME,
              BrokerConfigUtils.deriveLocalProducerConfigs(originals))
      );

      // use 'confluent.balancer.topic.replication.factor' if set, otherwise use our default
      final String balanceReplicationFactor = BrokerConfigUtils.getBalanceReplicationFactor(originals);
      if (balanceReplicationFactor != null) {
        configs.putAll(
            prefixedExporterConfigs(
                ConfluentTelemetryConfig.EXPORTER_LOCAL_NAME,
                ImmutableMap.of(
                    KafkaExporterConfig.TOPIC_REPLICAS_CONFIG,
                    balanceReplicationFactor
                )
            )
        );
      }
    }

    // finally apply the originals
    configs.putAll(originals);

    return configs;
  }

  @VisibleForTesting
  Context getContext() {
    return this.ctx;
  }

  public Emitter emitter() {
    if (this.emitter == null) {
      throw new IllegalStateException("emitter() was called before the Emitter was instantiated.");
    }
    return this.emitter;
  }

  private synchronized Emitter createEmitter() {
    emitter = new TelemetryEmitter(ctx, exporters::values, unionPredicate, selfMetrics);
    return emitter;
  }

  @Override
  public EventEmitter eventEmitter() {
    return this.eventEmitterOpt.isPresent() ? this.eventEmitterOpt.get() : NoOpEventEmitter.INSTANCE;
  }

  private void closeEventEmitter() {
    eventEmitterOpt.ifPresent(eventEmitter -> {
      try {
        eventEmitter.close();
      } catch (Exception e) {
        log.error("Error while closing {}", eventEmitter, e);
      }
    });
  }

  private Optional<EventEmitterImpl> createEventEmitter(Map<String, ?> configs) {
    Optional<EventEmitterImpl> eventEmitterOpt = Optional.empty();

    if (maybeInitEventEmitter(configs)) {
      eventEmitterOpt = Optional.of(new EventEmitterImpl(configs));
    }
    return eventEmitterOpt;
  }

  private void reconfigureEventEmitter(Map<String, ?> configs) {
    this.closeEventEmitter();
    this.eventEmitterOpt = createEventEmitter(configs);
    this.eventEmitterOpt.ifPresent(eventEmitter -> eventEmitter.setEventLabels(this.activeProvider.resource().getLabelsMap()));
  }

  private boolean maybeInitEventEmitter(Map<String, ?> configs) {
    EventEmitterConfig config = new EventEmitterConfig(configs);
    Map<String, Map<String, Object>> eventExporterConfigs = config.getEnabledExporterConfigs(EventEmitterConfig.EventType.events);
    if (eventExporterConfigs == null ||  eventExporterConfigs.isEmpty()) {
      return false;
    }
    for (Map.Entry<String, Map<String, Object>> entry : eventExporterConfigs.entrySet()) {
      //check if the exporter is an event exporter and the exporter is a kafka exporter,
      //since we only support kafka exporter for events
      if (config.isKafkaExporter(entry.getValue())) {
        return true;
      }
    }

    return false;
  }
}
