package org.apache.kafka.clients.telemetry.collector;

import org.apache.kafka.clients.telemetry.MetricKey;
import org.apache.kafka.clients.telemetry.emitter.Emitter;
import org.apache.kafka.clients.telemetry.metrics.SinglePointMetric;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

 public class KafkaMetricsCollector implements MetricsCollector {
    private static final Logger log = LoggerFactory.getLogger(KafkaMetricsCollector.class);
    private final StateLedger ledger;
    // MetricKey computation is relatively expensive, cache and re-use computed MetricKeys
    private final ConcurrentMap<MetricName, MetricKey> metricKeyCache;
    private final Clock clock;
    private final MetricNamingStrategy<MetricName> metricNamingStrategy;


    private static final Field METRIC_VALUE_PROVIDER_FIELD;

    static {
        try {
            METRIC_VALUE_PROVIDER_FIELD = KafkaMetric.class.getDeclaredField("metricValueProvider");
            METRIC_VALUE_PROVIDER_FIELD.setAccessible(true);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    public KafkaMetricsCollector(MetricNamingStrategy<MetricName> metricNamingStrategy) {
        this(metricNamingStrategy, Clock.systemUTC());
    }

    KafkaMetricsCollector(MetricNamingStrategy<MetricName> metricNamingStrategy, Clock clock) {
        this.metricNamingStrategy = metricNamingStrategy;
        this.clock = clock;
        ledger = new StateLedger();
        metricKeyCache = new ConcurrentHashMap<>();
    }

    public void init(List<KafkaMetric> metrics) {
        ledger.init(metrics);
    }

    /**
     * This is called whenever a metric is updated or added
     */
    public void metricChange(KafkaMetric metric) {
        ledger.metricChange(metric);
        metricKeyCache.remove(metric.metricName());
    }

    /**
     * This is called whenever a metric is removed
     */
    public void metricRemoval(KafkaMetric metric) {
        ledger.metricRemoval(metric);
        metricKeyCache.remove(metric.metricName());
    }

    Set<MetricName> getTrackedMetrics() {
        return ledger.metricMap.keySet();
    }

    @Override
    public void collect(Emitter emitter) {
        for (Map.Entry<MetricName, KafkaMetric> entry : ledger.getMetrics()) {
            MetricName originalMetricName = entry.getKey();
            KafkaMetric metric = entry.getValue();

            try {
                collectMetric(emitter, originalMetricName, metric);
            } catch (Exception e) {
                // catch and log to continue processing remaining metrics
                log.error("Unexpected error processing Kafka metric {}", originalMetricName, e);
            }
        }
    }

    private void collectMetric(Emitter emitter, MetricName originalMetricName, KafkaMetric metric) {
        MetricKey metricKey = toMetricKey(originalMetricName);
        String name = metricKey.getName();
        Map<String, String> labels = metricKey.getLabels();
        Object metricValue;
        try {
            metricValue = metric.metricValue();
        } catch (Exception e) {
            /**
             * If exception happens when retrieve value, log warning and continue to process the rest of metrics
             */
            log.warn("Failed to retrieve metric value {}", originalMetricName.name(), e);
            return;
        }

        if (isMeasurable(metric)) {
            Measurable measurable = metric.measurable();
            double value = (Double) metricValue;

            if (measurable instanceof WindowedCount || measurable instanceof CumulativeSum) {
                collectSum(name, labels, value, emitter);
                collectDelta(originalMetricName, name, labels, value, emitter);
            } else {
                collectGauge(name, labels, value, emitter);
            }
        } else {
            // It is non-measurable Gauge metric.
            // Collect the metric only if its value is a number.
            if (metricValue instanceof Number) {
                Number value = (Number) metricValue;
                if (value instanceof Integer || value instanceof Long) {
                    // map integer types to long
                    collectGauge(name, labels, value.longValue(), emitter);
                } else {
                    // map any other number type to double
                    collectGauge(name, labels, value.doubleValue(), emitter);
                }
            } else {
                // skip non-measurable metrics
                log.debug("Skipping non-measurable gauge metric {}", originalMetricName.name());
            }
        }
    }
    private void collectDelta(MetricName originalMetricName, String metricName, Map<String, String> labels,
                              Double value, Emitter emitter) {
        String deltaName = metricName + "/delta";
        MetricKey metricKey = new MetricKey(deltaName, labels);
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        // calculate a getAndSet, and add to out if non-empty
        final Instant t = Instant.now(clock);
        LastValueTracker.InstantAndValue<Double> instantAndValue = ledger.delta(originalMetricName, t, value);

        emitter.emitMetric(
                SinglePointMetric.deltaSum(metricKey, instantAndValue.getValue(), t, instantAndValue.getIntervalStart())
        );
    }

    private void collectSum(String metricName, Map<String, String> labels, double value, Emitter emitter) {
        MetricKey metricKey = new MetricKey(metricName, labels);
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
                SinglePointMetric.sum(metricKey, value, Instant.now(clock))
        );
    }

    private void collectSum(String metricName, Map<String, String> labels, long value, Emitter emitter) {
        MetricKey metricKey = new MetricKey(metricName, labels);
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
                SinglePointMetric.sum(metricKey, value, Instant.now(clock))
        );
    }

    private void collectGauge(String metricName, Map<String, String> labels, double value, Emitter emitter) {
        MetricKey metricKey = new MetricKey(metricName, labels);
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
                SinglePointMetric.gauge(metricKey, value, Instant.now(clock))
        );
    }

    private void collectGauge(String metricName, Map<String, String> labels, long value, Emitter emitter) {
        MetricKey metricKey = new MetricKey(metricName, labels);
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
                SinglePointMetric.gauge(metricKey, value, Instant.now(clock))
        );
    }

    @Override
    public String toString() {
        return this.getClass().getCanonicalName();
    }

    private static boolean isMeasurable(KafkaMetric metric) {
        // KafkaMetric does not expose the internal MetricValueProvider and throws an IllegalStateException exception
        // if .measurable() is called for a Gauge.
        // There are 2 ways to find the type of internal MetricValueProvider for a KafkaMetric - use reflection or
        // get the information based on whether or not a IllegalStateException exception is thrown.
        // We use reflection so that we can avoid the cost of generating the stack trace when it's
        // not a measurable.
        try {
            Object provider = METRIC_VALUE_PROVIDER_FIELD.get(metric);
            return provider instanceof Measurable;
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    // Package private for testing.
    MetricKey toMetricKey(MetricName metricName) {
        return metricKeyCache.computeIfAbsent(metricName, metricNamingStrategy::metricKey);
    }

    /**
     * Keeps track of the state of metrics, e.g. when they were added, what their getAndSet value is,
     * and clearing them out when they're removed.
     *
     * <p>Note that this class doesn't have a context object, so it can't use the real
     * MetricKey (with context.labels()). The StateLedger is created earlier in the process so
     * that it can handle the MetricsReporter methods (init/metricChange,metricRemoval).</p>
     */
    private class StateLedger {

        private final Map<MetricName, KafkaMetric> metricMap = new ConcurrentHashMap<>();
        private final LastValueTracker<Double> doubleDeltas = new LastValueTracker<>();
        private final ConcurrentMap<MetricName, Instant> metricAdded = new ConcurrentHashMap<>();

        private Instant instantAdded(MetricName metricName) {
            // lookup when the metric was added to use it as the interval start. That should always
            // exist, but if it doesn't (e.g. if there's a race) then we use now.
            return metricAdded.computeIfAbsent(metricName, x -> Instant.now(clock));
        }

        // package private for testing.
        MetricKey toKey(MetricName metricName) {
            return new MetricKey(metricName.toString(), Collections.emptyMap());
        }

        public void init(List<KafkaMetric> metrics) {
            log.debug("initializing Kafka metrics collector");
            for (KafkaMetric m : metrics) {
                metricMap.put(m.metricName(), m);
            }
        }

        public void metricChange(KafkaMetric metric) {
            metricMap.put(metric.metricName(), metric);
            if (doubleDeltas.contains(toKey(metric.metricName()))) {
                log.warn("Registering a new metric {} which already has a last value tracked. " +
                        "Removing metric from delta register.", metric.metricName(), new Exception());

                // This scenario shouldn't occur while registering a metric since it should
                // have already been cleared out on cleanup/shutdown.
                // We remove the metric here to clear out the delta register because we are running
                // into an issue where old metrics are being re-registered which causes us to
                // record a negative delta
                doubleDeltas.remove(toKey(metric.metricName()));
            }
            metricAdded.put(metric.metricName(), Instant.now(clock));
        }

        public void metricRemoval(KafkaMetric metric) {
            log.debug("removing kafka metric : {}", metric.metricName());
            metricMap.remove(metric.metricName());
            doubleDeltas.remove(toKey(metric.metricName()));
            metricAdded.remove(metric.metricName());
        }

        public Iterable<? extends Entry<MetricName, KafkaMetric>> getMetrics() {
            return metricMap.entrySet();
        }

        public LastValueTracker.InstantAndValue<Double> delta(MetricName metricName, Instant now, Double value) {
           return null;
        }
    }
}
