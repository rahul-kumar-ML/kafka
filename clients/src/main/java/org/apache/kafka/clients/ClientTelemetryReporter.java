package org.apache.kafka.clients;

import static org.apache.kafka.common.Uuid.ZERO_UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.telemetry.collector.KafkaMetricsCollector;
import org.apache.kafka.common.telemetry.collector.MetricsCollector;
import org.apache.kafka.common.telemetry.emitter.Context;
import org.apache.kafka.common.telemetry.metrics.MetricKeyable;
import org.apache.kafka.common.telemetry.metrics.MetricNamingConvention;
import org.apache.kafka.common.telemetry.metrics.SinglePointMetric;
import org.apache.kafka.common.telemetry.provider.KafkaClientProvider;
import org.apache.kafka.common.telemetry.provider.Provider;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of the client telemetry reporter.
 *
 * <p/>
 *
 * The full life-cycle of the metric collection process is defined by a state machine in
 * {@link ClientTelemetryState}. Each state is associated with a different set of operations.
 * For example, the client telemetry manager will attempt to fetch the telemetry subscription
 * from the broker when in the {@link ClientTelemetryState#SUBSCRIPTION_NEEDED} state.
 * If the push operation failed, the client telemetry manager will attempt to re-fetch the
 * subscription information by setting the state back to
 * {@link ClientTelemetryState#SUBSCRIPTION_NEEDED}.
 *
 * <p/>
 *
 * In an unlikely scenario, if a bad state transition is detected, an
 * {@link IllegalClientTelemetryStateException} will be thrown.
 *
 * <p/>
 *
 * The state transition follows the following steps in order:
 *
 * <ol>
 *     <li>{@link ClientTelemetryState#SUBSCRIPTION_NEEDED}</li>
 *     <li>{@link ClientTelemetryState#SUBSCRIPTION_IN_PROGRESS}</li>
 *     <li>{@link ClientTelemetryState#PUSH_NEEDED}</li>
 *     <li>{@link ClientTelemetryState#PUSH_IN_PROGRESS}</li>
 *     <li>{@link ClientTelemetryState#TERMINATING_PUSH_NEEDED}</li>
 *     <li>{@link ClientTelemetryState#TERMINATING_PUSH_IN_PROGRESS}</li>
 *     <li>{@link ClientTelemetryState#TERMINATED}</li>
 * </ol>
 *
 * <p/>
 *
 * For more detail in state transition, see {@link ClientTelemetryState#validateTransition}.
 */
public class ClientTelemetryReporter implements MetricsReporter {

  private static final Logger log = LoggerFactory.getLogger(ClientTelemetryReporter.class);

  public enum ConnectionErrorReason {
    AUTH, CLOSE, DISCONNECT, TIMEOUT, TLS
  }

  public enum RequestErrorReason {
    DISCONNECT, ERROR, TIMEOUT
  }

  public final static int DEFAULT_PUSH_INTERVAL_MS = 30_000;
  public final static long MAX_TERMINAL_PUSH_WAIT_MS = 100;
  public static final String PREFIX_LABELS = "confluent.telemetry.labels.";

//  static final Predicate<KafkaMetric> KAFKA_METRIC_PREDICATE = m -> m.metricName().name().startsWith("org.apache.kafka");

  private static final Predicate<KafkaMetric> KAFKA_METRIC_PREDICATE = m -> true;
  private final List<MetricsCollector> collectors = new CopyOnWriteArrayList<>();
  private final Provider activeProvider = new KafkaClientProvider();

  private final ClientTelemetrySender clientTelemetrySender;
  private Map<String, Object> rawOriginalConfig;
  private volatile KafkaMetricsCollector kafkaMetricsCollector;
  private volatile Context ctx;

  public ClientTelemetryReporter() {
    this.clientTelemetrySender = new DefaultClientTelemetrySender();
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void configure(Map<String, ?> configs) {
    rawOriginalConfig = (Map<String, Object>) configs;
//    initContext();
//    initCollectors();
  }

  @Override
  public synchronized void contextChange(MetricsContext metricsContext) {
    log.info("metricsContext {}", metricsContext.contextLabels());
    /**
     * If validation succeeds: initialize the provider, start the metric collection task, set metrics labels for services/libraries that expose metrics
     */
    Preconditions.checkState(this.rawOriginalConfig != null, "configure() was not called before contextChange()");
    if (!metricsContext.contextLabels().containsKey(MetricsContext.NAMESPACE)) {
      log.error("_namespace not found in metrics context. Metrics collection is disabled");
      return;
    }

    collectors.forEach(MetricsCollector::stop);

    log.info("provider {} is selected.", activeProvider.getClass().getCanonicalName());

    if (!activeProvider.validate(metricsContext, rawOriginalConfig)) {
      log.warn("Validation failed for {} context {}", activeProvider.getClass(), metricsContext.contextLabels());
     return;
    }

    if (kafkaMetricsCollector == null) {
      // Initialize the provider only once. contextChange(..) can be called more than once,
      //but once it's been initialized and all necessary labels are present then we don't re-initialize again.
      activeProvider.configure(rawOriginalConfig);
    }
    activeProvider.contextChange(metricsContext);

    if (ctx == null) {
      initContext();
    }

    if (kafkaMetricsCollector == null) {
      initCollectors();
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

  @Override
  public ClientTelemetrySender clientSender() {
    return clientTelemetrySender;
  }

  @Override
  public void close() {
    log.trace("close");
    // Do nothing, may be close client telemetry
  }

  private void initContext() {
    Resource.Builder builder = activeProvider.resource().toBuilder();
    //add labels defined in properties file to resource: confluent.telemetry.labels.*
    getLabels().forEach((k, v) -> builder.addAttributesBuilder()
        .setKey(k)
        .setValue(AnyValue.newBuilder().setStringValue(v)));
    Resource resource = builder.build();
    ctx = new Context(
        resource,
        activeProvider.domain()
    );
  }

  private Map<String, String> getLabels() {
    Map<String, String> labels = new HashMap<>();
    for (Map.Entry<String, ?> entry : rawOriginalConfig.entrySet()) {
      if (entry.getKey().startsWith(PREFIX_LABELS)) {
        labels.put(entry.getKey().substring(PREFIX_LABELS.length()), (String) entry.getValue());
      }
    }
    return labels;
  }

  private void initCollectors() {
    kafkaMetricsCollector = new KafkaMetricsCollector(
        MetricNamingConvention.getMetricNamingStartegy(activeProvider.domain()), Clock.systemUTC(), KAFKA_METRIC_PREDICATE
    );
    collectors.add(kafkaMetricsCollector);
    collectors.addAll(activeProvider.extraCollectors(ctx));
  }

  public class DefaultClientTelemetrySender implements ClientTelemetrySender {

    // These are the lower and upper bounds of the jitter that we apply to the initial push
    // telemetry API call. This helps to avoid a flood of requests all coming at the same time.
    private final static double INITIAL_PUSH_JITTER_LOWER = 0.5;
    private final static double INITIAL_PUSH_JITTER_UPPER = 1.5;

    private final ReadWriteLock instanceStateLock = new ReentrantReadWriteLock();
    private final Condition subscriptionLoaded = instanceStateLock.writeLock().newCondition();
    private final Condition terminalPushInProgress = instanceStateLock.writeLock().newCondition();
    private ClientTelemetryState state = ClientTelemetryState.SUBSCRIPTION_NEEDED;

    private final Set<ClientTelemetryListener> listeners;
    private final Clock clock;

    private ClientTelemetrySubscription subscription;
    private long lastSubscriptionFetchMs;
    private long pushIntervalMs;

    private DefaultClientTelemetrySender() {
      this.listeners = new HashSet<>();
      this.clock = Clock.systemUTC();
    }

    @Override
    public long timeToNextUpdate(long requestTimeoutMs) {
      ClientTelemetryState localState;
      long localLastSubscriptionFetchMs;
      long localPushIntervalMs;

      try {
        instanceStateLock.readLock().lock();
        localState = state;
        localLastSubscriptionFetchMs = lastSubscriptionFetchMs;
        localPushIntervalMs = pushIntervalMs;
      } finally {
        instanceStateLock.readLock().unlock();
      }

      final long timeMs;
      final String msg;

      switch (localState) {
        case SUBSCRIPTION_IN_PROGRESS:
        case PUSH_IN_PROGRESS:
        case TERMINATING_PUSH_IN_PROGRESS: {
          // We have a network request in progress. We record the time of the request
          // submission, so wait that amount of the time PLUS the requestTimeout that
          // is provided.
          String apiName = (localState == ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS) ? ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.name : ApiKeys.PUSH_TELEMETRY.name;
          timeMs = requestTimeoutMs;
          msg = String.format("this is the remaining wait time for the %s network API request, as specified by %s", apiName, CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG);
          break;
        }

        case TERMINATING_PUSH_NEEDED: {
          timeMs = 0;
          msg = String.format("this is to give the client a chance to submit the final %s network API request ASAP before closing", ApiKeys.PUSH_TELEMETRY.name);
          break;
        }

        case SUBSCRIPTION_NEEDED:
        case PUSH_NEEDED:
          long timeRemainingBeforeRequest = localLastSubscriptionFetchMs + localPushIntervalMs - clock.millis();
          String apiName = localState == ClientTelemetryState.SUBSCRIPTION_NEEDED ? ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS.name : ApiKeys.PUSH_TELEMETRY.name;

          if (timeRemainingBeforeRequest < 0) {
            timeMs = 0;
            msg = String.format("the wait time before submitting the next %s network API request has elapsed", apiName);
          } else {
            timeMs = timeRemainingBeforeRequest;
            msg = String.format("this is the time the client will wait before submitting the next %s network API request", apiName);
          }
          break;

        default:
          // Should never get to here
          timeMs = Long.MAX_VALUE;
          msg = "this should not happen and likely signals an error";
      }

      log.trace("For telemetry state {}, returning the value {} ms; {}", localState, timeMs, msg);
      return timeMs;
    }

    @Override
    public Optional<AbstractRequest.Builder<?>> createRequest() {
      log.info("[APM] - create request - telemetry");
      ClientTelemetryState localState;
      ClientTelemetrySubscription localSubscription;

      try {
        instanceStateLock.readLock().lock();
        localState = state;
        localSubscription = subscription;
      } finally {
        instanceStateLock.readLock().unlock();
      }

      if (localState == ClientTelemetryState.SUBSCRIPTION_NEEDED) {
        log.info("[APM] - subscription needed");
        return createSubscriptionRequest(localSubscription);
      } else if (localState == ClientTelemetryState.PUSH_NEEDED || localState == ClientTelemetryState.TERMINATING_PUSH_NEEDED) {
        return createPushRequest(localSubscription);
      }

      log.warn("[APM] - Cannot make telemetry request as telemetry is in state: {}", localState);
      return Optional.empty();
    }

    @Override
    public void handleResponse(GetTelemetrySubscriptionsResponseData data) {
      log.info("[APM] - handleResponse - GetTelemetrySubscriptionResponse: {}", data);
      Optional<Long> pushIntervalMsOpt = ClientTelemetryUtils.errorPushIntervalMs(data.errorCode());

      // This is the error case...
      if (pushIntervalMsOpt.isPresent()) {
        setFetchAndPushInterval(pushIntervalMsOpt.get());
        return;
      }

      // This is the success case...
      log.info("Successfully retrieved telemetry subscription; response: {}", data);
      List<String> requestedMetrics = data.requestedMetrics();
      Predicate<? super MetricKeyable> selector = ClientTelemetryUtils.getSelectorFromRequestedMetrics(requestedMetrics);
      List<CompressionType> acceptedCompressionTypes = ClientTelemetryUtils.getCompressionTypesFromAcceptedList(data.acceptedCompressionTypes());
      Uuid clientInstanceId = ClientTelemetryUtils.validateClientInstanceId(data.clientInstanceId());
      int pushIntervalMs = getPushIntervalMs(data);

      ClientTelemetrySubscription clientTelemetrySubscription = new ClientTelemetrySubscription(data.throttleTimeMs(),
          clientInstanceId,
          data.subscriptionId(),
          acceptedCompressionTypes,
          pushIntervalMs,
          data.deltaTemporality(),
          selector);

      log.info("Successfully retrieved telemetry subscription: {}", clientTelemetrySubscription);

      try {
        instanceStateLock.writeLock().lock();
        // This is the case if we began termination sometime after the subscription request
        // was issued. We're just now getting our callback, but we need to ignore it.
        if (isTerminatingState()) {
          return;
        }

        ClientTelemetryState newState;
        if (selector == ClientTelemetryUtils.SELECTOR_NO_METRICS) {
          // This is the case where no metrics are requested and/or match the filters. We need
          // to wait pushIntervalMs then retry.
          newState = ClientTelemetryState.SUBSCRIPTION_NEEDED;
        } else {
          newState = ClientTelemetryState.PUSH_NEEDED;
        }

        // If we're terminating, don't update state or set the subscription or update
        // any of the listeners.
        if (!maybeSetState(newState)) {
          return;
        }

        setSubscription(clientTelemetrySubscription);
      } finally {
        instanceStateLock.writeLock().unlock();
      }

      invokeListeners(l -> l.getSubscriptionCompleted(clientTelemetrySubscription));
    }

    @Override
    public void handleResponse(PushTelemetryResponseData data) {
      log.info("[APM] - handleResponse - PushTelemetryResponse: {}", data);
      Optional<Long> pushIntervalMsOpt = ClientTelemetryUtils.errorPushIntervalMs(data.errorCode());

      // This is the success case...
      if (!pushIntervalMsOpt.isPresent()) {
        log.debug("Successfully pushed telemetry; response: {}", data);
        ClientTelemetrySubscription localSubscription;
        try {
          instanceStateLock.readLock().lock();
          localSubscription = subscription;
        } finally {
          instanceStateLock.readLock().unlock();
        }
        invokeListeners(l -> l.pushRequestCompleted(localSubscription.clientInstanceId()));
      }

      // This is the error case...
      try {
        instanceStateLock.writeLock().lock();
        // This is the case if we began termination sometime after the last push request
        // was issued. We're just now getting our callback, but we need to ignore it as all
        // of our state has already been updated.
        if (isTerminatingState()) {
          return;
        }

        ClientTelemetryState newState;
        if (state == ClientTelemetryState.PUSH_IN_PROGRESS) {
          newState = ClientTelemetryState.SUBSCRIPTION_NEEDED;
        } else {
          log.warn("Could not transition state after failed push telemetry from state {}", state);
          return;
        }

        if (!maybeSetState(newState)) {
          return;
        }
        pushIntervalMsOpt.ifPresent(this::setFetchAndPushInterval);
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    @Override
    public void handleFailedRequest(ApiKeys apiKey, KafkaException maybeFatalException) {
      log.info("[APM] - handleFailedRequest");
      log.warn("The broker generated an error for the {} network API request", apiKey.name, maybeFatalException);

      try {
        instanceStateLock.writeLock().lock();
        if (isTerminatingState()) {
          return;
        }

        boolean shouldWait = (maybeFatalException != null);
        ClientTelemetryState newState;

        if (apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS) {
          newState = ClientTelemetryState.SUBSCRIPTION_NEEDED;
        } else if (apiKey == ApiKeys.PUSH_TELEMETRY) {
          if (state == ClientTelemetryState.PUSH_IN_PROGRESS) {
            newState = ClientTelemetryState.SUBSCRIPTION_NEEDED;
          } else {
            log.warn("Could not transition state after failed push telemetry from state {}", state);
            return;
          }
        } else {
          return;
        }

        if (!maybeSetState(newState)) {
          return;
        }

        // The broker does not support telemetry. Let's sleep for a while before
        // trying things again. We may disconnect from the broker and connect to
        // a broker that supports client telemetry.
        if (shouldWait) {
          setFetchAndPushInterval(DEFAULT_PUSH_INTERVAL_MS);
        }
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    @Override
    public void close() {
      log.trace("close");

      log.info("Stopping ClientTelemetryReporter collectorTask");
      collectors.forEach(MetricsCollector::stop);
      boolean shouldClose = false;
      try {
        instanceStateLock.writeLock().lock();
        if (this.state != ClientTelemetryState.TERMINATED) {
          if (maybeSetState(ClientTelemetryState.TERMINATED)) {
            shouldClose = true;
          }
        } else {
          log.debug("Ignoring subsequent close");
        }
      } finally {
        instanceStateLock.writeLock().unlock();
      }

      if (shouldClose) {
        for (MetricsCollector mc : collectors) {
          mc.stop();
        }
      }
    }

    private Optional<Builder<?>> createSubscriptionRequest(ClientTelemetrySubscription localSubscription) {
      // If we've previously retrieved a subscription, it will contain the client instance ID
      // that the broker assigned. Otherwise, per KIP-714, we send a special "null" UUID to
      // signal to the broker that we need to have a client instance ID assigned.
      Uuid clientInstanceId = (localSubscription != null) ? localSubscription.clientInstanceId() : ZERO_UUID;

      try {
        instanceStateLock.writeLock().lock();
        if (isTerminatingState()) {
          return Optional.empty();
        }

        if (!maybeSetState(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS))
          return Optional.empty();
      } finally {
        instanceStateLock.writeLock().unlock();
      }

      AbstractRequest.Builder<?> requestBuilder = new GetTelemetrySubscriptionRequest.Builder(clientInstanceId);
      log.info("[APM] - createRequest - created new subscription {} network API request", requestBuilder.apiKey().name);
      invokeListeners(l -> l.getSubscriptionRequestCreated(clientInstanceId));
      return Optional.of(requestBuilder);
    }

    private Optional<Builder<?>> createPushRequest(ClientTelemetrySubscription localSubscription) {
      log.info("[APM] - telemetry push");
      if (localSubscription == null) {
        log.warn("[APM] - Telemetry state is {} but subscription is null; not sending telemetry", state);
        return Optional.empty();
      }

      boolean terminating;
      try {
        instanceStateLock.writeLock().lock();
        // We've already been terminated, or we've already issued our last push, so we
        // should just exit now.
        if (this.state == ClientTelemetryState.TERMINATED || this.state == ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS) {
          return Optional.empty();
        }

        // Check the *actual* state (while locked) to make sure we're still in the state
        // we expect to be in.
        terminating = (state == ClientTelemetryState.TERMINATING_PUSH_NEEDED);
        if (!maybeSetState(terminating ? ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS : ClientTelemetryState.PUSH_IN_PROGRESS)) {
          return Optional.empty();
        }
      } finally {
        instanceStateLock.writeLock().unlock();
      }

      List<SinglePointMetric> emitted;
      byte[] payload;

      try (ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(localSubscription.selector(), ctx)) {
        emitter.init();

        for (MetricsCollector mc : collectors)
          mc.collect(emitter);

        payload = emitter.payload();
        emitted = emitter.emitted();
      }

      CompressionType compressionType = ClientTelemetryUtils.preferredCompressionType(localSubscription.acceptedCompressionTypes());
      log.info("Compression type: {}", compressionType);
      compressionType = CompressionType.NONE;
      log.info("Changed compression type: {}", compressionType);
      ByteBuffer buf = ClientTelemetryUtils.serialize(payload, compressionType);
      Bytes metricsData =  Bytes.wrap(Utils.readBytes(buf));

      AbstractRequest.Builder<?> requestBuilder = new PushTelemetryRequest.Builder(localSubscription.clientInstanceId(),
          localSubscription.subscriptionId(),
          terminating,
          compressionType,
          metricsData);

      log.info("[APM] - createRequest - created new telemetry {} network API request", requestBuilder.apiKey().name);

      final CompressionType finalCompressionType = compressionType;
      invokeListeners(l -> l.pushRequestCreated(localSubscription.clientInstanceId(),
          localSubscription.subscriptionId(),
          terminating,
          finalCompressionType,
          emitted));
      return Optional.of(requestBuilder);
    }

    private int getPushIntervalMs(GetTelemetrySubscriptionsResponseData data) {
      final int pushIntervalMs = ClientTelemetryUtils.validatePushIntervalMs(data.pushIntervalMs() > 0 ? data.pushIntervalMs() : DEFAULT_PUSH_INTERVAL_MS);

      try {
        instanceStateLock.readLock().lock();
        // If lastSubscriptionFetchMs is not zero hence it's not first request.
        if (lastSubscriptionFetchMs != 0) {
          return pushIntervalMs;
        }
      } finally {
        instanceStateLock.readLock().unlock();
      }

      // If this is the first request from this client, we want to attempt to spread out
      // the push requests between 50% and 150% of the push interval value from the broker.
      // This helps us to avoid the case where multiple clients are started at the same time
      // and end up sending all their data at the same time.
      double rand = ThreadLocalRandom.current().nextDouble(INITIAL_PUSH_JITTER_LOWER, INITIAL_PUSH_JITTER_UPPER);
      int firstPushIntervalMs = (int) Math.round(rand * pushIntervalMs);

      log.info("Telemetry subscription push interval value from broker was {}; to stagger requests the first push"
                + " interval is being adjusted to {}", pushIntervalMs, firstPushIntervalMs);
      return firstPushIntervalMs;
    }

    /**
     * Updates the {@link ClientTelemetrySubscription}, {@link #pushIntervalMs}, and
     * {@link #lastSubscriptionFetchMs}.
     *
     * <p/>
     *
     * The entire contents of the method are guarded by the {@link #instanceStateLock}.
     *
     * <p/>
     *
     * After the update, the {@link #subscriptionLoaded} condition is signaled so any threads
     * waiting on the subscription can be unblocked.
     *
     * @param subscription Updated subscription that will replace any current subscription
     */
    @VisibleForTesting
    void setSubscription(ClientTelemetrySubscription subscription) {
      try {
        instanceStateLock.writeLock().lock();

        this.subscription = subscription;
        this.pushIntervalMs = subscription.pushIntervalMs();
        this.lastSubscriptionFetchMs = clock.millis();

        log.trace("Updating subscription - subscription: {}; pushIntervalMs: {}, lastFetchMs: {}", subscription, pushIntervalMs, lastSubscriptionFetchMs);

        // In some cases we have to wait for this signal in the clientInstanceId method so that
        // we know that we have a subscription to pull from.
        subscriptionLoaded.signalAll();
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    /**
     * Updates <em>only</em> the {@link #pushIntervalMs} and {@link #lastSubscriptionFetchMs}.
     *
     * <p/>
     *
     * The entire contents of the method are guarded by the {@link #instanceStateLock}.
     *
     * <p/>
     *
     * No condition is signaled here, unlike {@link #setSubscription(ClientTelemetrySubscription)}.
     */
    private void setFetchAndPushInterval(long pushIntervalMs) {
      try {
        instanceStateLock.writeLock().lock();
        this.pushIntervalMs = pushIntervalMs;
        this.lastSubscriptionFetchMs = clock.millis();

        log.trace("Updating pushIntervalMs: {}, lastFetchMs: {}", pushIntervalMs, lastSubscriptionFetchMs);
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * the specific enclosing client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destination(s).
     *
     * This method waits up to <code>timeout</code> for the subscription to become available in
     * order to complete the request.
     *
     * @param timeout The maximum time to wait for enclosing client instance to determine its
     *                client instance ID. The value should be non-negative. Specifying a timeout
     *                of zero means do not wait for the initial request to complete if it hasn't
     *                already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        enclosing client instance is otherwise unusable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     * @return Human-readable string representation of the client instance ID
     */

    public Optional<String> clientInstanceId(Duration timeout) {
      log.trace("clientInstanceId");

      long timeoutMs = timeout.toMillis();
      if (timeoutMs < 0) {
        throw new IllegalArgumentException("The timeout cannot be negative.");
      }

      try {
        instanceStateLock.writeLock().lock();
        if (subscription == null) {
          // If we have a non-negative timeout and no-subscription, let's wait for one to
          // be retrieved.
          log.trace("Waiting for telemetry subscription containing the client instance ID with timeoutMillis = {} ms.", timeoutMs);
          try {
            if (!subscriptionLoaded.await(timeoutMs, TimeUnit.MILLISECONDS))
              log.debug("Wait for telemetry subscription elapsed; may not have actually loaded it");
          } catch (InterruptedException e) {
            throw new InterruptException(e);
          }
        }

        if (subscription == null) {
          log.debug("Client instance ID could not be retrieved with timeout {}", timeout);
          return Optional.empty();
        }

        Uuid clientInstanceId = subscription.clientInstanceId();
        if (clientInstanceId == null) {
          log.debug("Client instance ID was null in telemetry subscription while in state {}", state);
          return Optional.empty();
        }

        return Optional.of(clientInstanceId.toString());
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    private boolean isTerminatingState() {
      return state == ClientTelemetryState.TERMINATED || state == ClientTelemetryState.TERMINATING_PUSH_NEEDED
          || state == ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS;
    }

    @VisibleForTesting
    boolean maybeSetState(ClientTelemetryState newState) {
      try {
        instanceStateLock.writeLock().lock();
        ClientTelemetryState oldState = state;
        state = oldState.validateTransition(newState);
        log.trace("Setting telemetry state from {} to {}", oldState, newState);

        if (newState == ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS) {
          terminalPushInProgress.signalAll();
        }
        return true;
      } catch (IllegalClientTelemetryStateException e) {
        log.warn("Error updating client telemetry state", e);
        return false;
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    @VisibleForTesting
    ClientTelemetryState state() {
      try {
        instanceStateLock.readLock().lock();
        return state;
      } finally {
        instanceStateLock.readLock().unlock();
      }
    }

    public void initiateClose(Duration timeout) {
      log.info("initiateClose");

      long timeoutMs = timeout.toMillis();
      if (timeoutMs < 0)
        throw new IllegalArgumentException("The timeout cannot be negative.");

      try {
        instanceStateLock.writeLock().lock();
        // If we never fetched a subscription, we can't really push anything.
        if (lastSubscriptionFetchMs == 0) {
          log.info("Telemetry subscription not loaded, not attempting terminating push");
          return;
        }

        if (isTerminatingState() || !maybeSetState(ClientTelemetryState.TERMINATING_PUSH_NEEDED)) {
          log.info("Ignoring subsequent initiateClose");
          return;
        }

        // If we never fetched a subscription, we can't really push anything.
        if (lastSubscriptionFetchMs == 0) {
          log.info("Telemetry subscription not loaded, not attempting terminating push");
          return;
        }

        try {
          log.info("About to wait {} ms. for terminal telemetry push to be submitted", timeout);

          if (!terminalPushInProgress.await(timeoutMs, TimeUnit.MILLISECONDS))
            log.info("Wait for terminal telemetry push to be submitted has elapsed; may not have actually sent request");
        } catch (InterruptedException e) {
          log.warn("Error during client telemetry close", e);
        }
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    public void addListener(ClientTelemetryListener listener) {
      try {
        instanceStateLock.writeLock().lock();
        this.listeners.add(listener);
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    public void removeListener(ClientTelemetryListener listener) {
      try {
        instanceStateLock.writeLock().lock();
        this.listeners.remove(listener);
      } finally {
        instanceStateLock.writeLock().unlock();
      }
    }

    private void invokeListeners(Consumer<ClientTelemetryListener> c) {
      List<ClientTelemetryListener> l;

      try {
        instanceStateLock.readLock().lock();
        l = new ArrayList<>(listeners);
      } finally {
        instanceStateLock.readLock().unlock();
      }

      if (!l.isEmpty()) {
        for (ClientTelemetryListener listener : l) {
          try {
            c.accept(listener);
          } catch (Throwable t) {
            log.warn(t.getMessage(), t);
          }
        }
      }
    }
  }
}
