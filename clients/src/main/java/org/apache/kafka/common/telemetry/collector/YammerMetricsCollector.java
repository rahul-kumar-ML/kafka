package org.apache.kafka.common.telemetry.collector;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint;
import io.opentelemetry.proto.metrics.v1.SummaryDataPoint.ValueAtQuantile;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.telemetry.MetricsUtils;
import org.apache.kafka.common.telemetry.emitter.Emitter;
import org.apache.kafka.common.telemetry.metrics.MetricKey;
import org.apache.kafka.common.telemetry.metrics.MetricNamingStrategy;
import org.apache.kafka.common.telemetry.metrics.SinglePointMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Yammer -> OpenTelemetry is based on : https://github.com/census-instrumentation/opencensus-java/blob/master/contrib/dropwizard/src/main/java/io/opencensus/contrib/dropwizard/DropWizardMetrics.java
public class YammerMetricsCollector implements MetricsCollector {
    private static final Logger log = LoggerFactory.getLogger(YammerMetricsCollector.class);

    static final String DEFAULT_UNIT = "1";
    static final String NS_UNIT = "ns";
    private static final double[] QUANTILES = {.5, .75, .95, .98, .99, .999};

    private final MetricsRegistry metricsRegistry;
    private final MetricNamingStrategy<MetricName> metricNamingStrategy;
    private final Clock clock;
    private final LastValueTracker<Long> longDeltas;
    private final LastValueTracker<Double> doubleDeltas;

    private final ConcurrentMap<MetricKey, Instant> metricAdded = new ConcurrentHashMap<>();
    // MetricKey computation is relatively expensive, cache and re-use computed MetricKeys
    private final ConcurrentMap<MetricName, MetricKey> metricKeyCache = new ConcurrentHashMap<>();

    public YammerMetricsCollector(MetricsRegistry metricsRegistry, MetricNamingStrategy<MetricName> metricNamingStrategy, LastValueTracker<Long> longDeltas, LastValueTracker<Double> doubleDeltas, Clock clock) {
        this.metricsRegistry = metricsRegistry;
        this.metricNamingStrategy = metricNamingStrategy;
        this.clock = clock;
        this.longDeltas = longDeltas;
        this.doubleDeltas = doubleDeltas;

        this.setupMetricListener();
    }

    private void setupMetricListener() {
        metricsRegistry.addListener(new MetricsRegistryListener() {
            @Override
            public void onMetricAdded(MetricName name, Metric metric) {
                metricAdded.put(toMetricKey(name), Instant.now(clock));
            }

            @Override
            public void onMetricRemoved(MetricName metricName) {
                MetricKey key = toMetricKey(metricName);
                longDeltas.remove(key);
                doubleDeltas.remove(key);
                metricAdded.remove(key);
                metricKeyCache.remove(metricName);
            }
        });
    }

    // package private for testing.
    MetricKey toMetricKey(MetricName metricName) {
        return metricKeyCache.computeIfAbsent(metricName, metricNamingStrategy::metricKey);
    }

    @Override
    public void collect(Emitter emitter) {
        log.info("[APM] - collect yammer metrics, ledger: {}", metricsRegistry.allMetrics().entrySet());
        Set<Map.Entry<MetricName, Metric>> metrics = metricsRegistry.allMetrics().entrySet();

        for (Map.Entry<MetricName, Metric> entry : metrics) {

            MetricName metricName = entry.getKey();
            Metric metric = entry.getValue();

            try {
                collectMetric(emitter, metricName, metric);
            } catch (Exception e) {
                // catch and log to continue processing remaining metrics
                log.error("Unexpected error processing Yammer metric {}", metricName, e);
            }
        }
    }

    private void collectMetric(Emitter emitter, MetricName metricName, Metric metric) {
        log.info("[APM] - collect yammer metric: {}", metricName);
        MetricKey metricKey = toMetricKey(metricName);
        Instant metricAddedInstant = instantAdded(metricKey);

        log.trace("Processing {}", metricName);

        if (metric instanceof Gauge) {
            collectGauge(metricKey, ((Gauge<?>) metric).value(), emitter);
        } else if (metric instanceof Counter) {
            long count = ((Counter) metric).count();
            collectCounter(metricKey, count, emitter);
            // Derived metric, results in a name like /delta.
            collectDelta(metricKey, count, metricAddedInstant, emitter);
        } else if (metric instanceof Meter) {
            // Only collect counters and 1min rate
            Meter meter = (Meter) metric;
            long count = meter.count();
            double rateOneMinute = meter.oneMinuteRate();

            MetricKey totalKey = metricNamingStrategy.derivedMetricKey(metricKey, "total");
            // Derived metric, results in a name like /total.
            collectMeter(totalKey, count, emitter);
            // Derived metric, results in a name like /total/delta.
            collectDelta(totalKey, count, metricAddedInstant, emitter);
            // Derived metric, results in a name like /rate/1_min.
            MetricKey rateKey = metricNamingStrategy.derivedMetricKey(metricKey, "rate");
            MetricKey rate1MinKey = metricNamingStrategy.derivedMetricKey(rateKey, "1_min");
            collectGauge(rate1MinKey, rateOneMinute, emitter);
        } else if (metric instanceof Timer) {
            collectTimer(metricKey, (Timer) metric, emitter);
            // Derived metric, results in a name like /time/delta
            MetricKey timeKey = metricNamingStrategy.derivedMetricKey(metricKey, "time");
            collectDelta(timeKey, ((Timer) metric).sum(), metricAddedInstant, emitter);
            // Derived metric, results in a name like /total/delta.
            MetricKey totalKey = metricNamingStrategy.derivedMetricKey(metricKey, "total");
            collectDelta(totalKey, ((Timer) metric).count(), metricAddedInstant, emitter);
        } else if (metric instanceof Histogram) {
            collectHistogram(metricKey, (Histogram) metric, emitter);
            // Derived metric, results in a name like /time/delta
            MetricKey timeKey = metricNamingStrategy.derivedMetricKey(metricKey, "time");
            collectDelta(timeKey, ((Histogram) metric).sum(), metricAddedInstant, emitter);
            // Derived metric, results in a name like /total/delta.
            MetricKey totalKey = metricNamingStrategy.derivedMetricKey(metricKey, "total");
            collectDelta(totalKey, ((Histogram) metric).count(), metricAddedInstant, emitter);

            // Collect histogram type
            MetricKey metricKey1 = new MetricKey(metricKey.name() + "_1", metricKey.tags());
            collectHistogram1(metricKey1, (Histogram) metric, emitter);
            // Derived metric, results in a name like /time/delta
            MetricKey timeKey1 = metricNamingStrategy.derivedMetricKey(metricKey1, "time");
            collectDelta(timeKey1, ((Histogram) metric).sum(), metricAddedInstant, emitter);
            // Derived metric, results in a name like /total/delta.
            MetricKey totalKey1 = metricNamingStrategy.derivedMetricKey(metricKey1, "total");
            collectDelta(totalKey1, ((Histogram) metric).count(), metricAddedInstant, emitter);

        } else {
            log.debug("Unexpected metric type for {}", metricName);
        }
    }

    private Instant instantAdded(MetricKey metricKey) {
        // lookup when the metric was added to use it as the interval start. That should always
        // exist, but if it doesn't (e.g. if there's a race) then we use now.
        return metricAdded.computeIfAbsent(metricKey, x -> clock.instant());
    }

    @Override
    public String toString() {
        return this.getClass().getCanonicalName();
    }

    private void collectGauge(MetricKey metricKey, Object gaugeValue,
                              Emitter emitter) {
        // Do not process the metric if metricKey does not match the metrics predicate.
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        // Figure out which gauge instance and call the right method to get value
        if (gaugeValue instanceof Number) {
            emitter.emitMetric(SinglePointMetric.gauge(metricKey, (Number) gaugeValue, clock.instant()));
        } else if (gaugeValue instanceof Boolean) {
            emitter.emitMetric(SinglePointMetric.gauge(metricKey, (Boolean) gaugeValue ? 1 : 0, clock.instant()));
        } else {
            // Ignoring Gauge (gauge.getKey()) with unhandled type.
            log.debug("Ignoring {} value = {}", metricKey.name(), gaugeValue);
        }

    }

    private void collectCounter(MetricKey metricKey, long counterValue, Emitter emitter) {
        // Do not process the metric if metricKey does not match the metrics predicate.
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
            SinglePointMetric.sum(metricKey, counterValue, clock.instant())
        );
    }


    private void collectDelta(MetricKey originalKey, long value, Instant metricAdded, Emitter emitter) {
        MetricKey metricKey = metricNamingStrategy.derivedMetricKey(originalKey, "delta");
        // Do not process the metric if metricKey does not match the metrics predicate.
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        // We key our delta look ups on the original metric name (not the modified one)
        Optional<InstantAndValue<Long>> lastValue = longDeltas.getAndSet(originalKey, clock.instant(), value);
        Instant start = metricAdded;
        long delta = value;
        if (lastValue.isPresent()) {
            start = lastValue.get().intervalStart();
            delta = value - lastValue.get().value();
        }

        emitter.emitMetric(
            SinglePointMetric.deltaSum(metricKey, delta, clock.instant(), start)
        );
    }

    private void collectDelta(MetricKey originalKey, double value, Instant metricAdded, Emitter emitter) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return;
        }

        MetricKey metricKey = metricNamingStrategy.derivedMetricKey(originalKey, "delta");
        // Do not process the metric if metricKey does not match the metrics predicate.
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        // We key our delta look ups on the original metric name (not the modified one)
        Optional<InstantAndValue<Double>> lastValue = doubleDeltas.getAndSet(originalKey, clock.instant(), value);
        Instant start = metricAdded;
        double delta = value;
        if (lastValue.isPresent()) {
            start = lastValue.get().intervalStart();
            delta = value - lastValue.get().value();
        }

        emitter.emitMetric(
            SinglePointMetric.deltaSum(metricKey, delta, Instant.now(clock), start)
        );
    }


    private void collectMeter(MetricKey metricKey, long meterCount, Emitter emitter) {
        // Do not process the metric if metricKey does not match the metrics predicate.
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
            SinglePointMetric.sum(metricKey, meterCount, clock.instant(), instantAdded(metricKey))
        );
    }

    private void collectHistogram(MetricKey metricKey, Histogram histogram, Emitter emitter) {
        // Do not process the metric if metricKey does not match the metrics predicate.
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
            collectSnapshotAndCount(metricKey, DEFAULT_UNIT, histogram.getSnapshot(), histogram.count(), histogram)
        );
    }

    private void collectHistogram1(MetricKey metricKey, Histogram histogram, Emitter emitter) {
        // Do not process the metric if metricKey does not match the metrics predicate.
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
            histogram1(metricKey, DEFAULT_UNIT, histogram.getSnapshot(), histogram.count(), histogram)
        );
    }

    private void collectTimer(MetricKey metricKey, Timer timer, Emitter emitter) {
        // Do not process the metric if metricKey does not match the metrics predicate.
        if (!emitter.shouldEmitMetric(metricKey)) {
            return;
        }

        emitter.emitMetric(
            collectSnapshotAndCount(metricKey, NS_UNIT, timer.getSnapshot(), timer.count(), timer)
        );
    }

    private SinglePointMetric collectSnapshotAndCount(
        MetricKey metricKey,
        String unit,
        Snapshot yammerSnapshot,
        long count,
        Summarizable summarizable) {
        SummaryDataPoint summary = SummaryDataPoint.newBuilder()
            .setTimeUnixNano(MetricsUtils.toTimeUnixNanos(clock.instant()))
            .addAllAttributes(SinglePointMetric.asAttributes(metricKey.tags()))
            .setCount(count)
            .setSum(summarizable.sum())
            .addQuantileValues(
                ValueAtQuantile.newBuilder().setQuantile(0)
                    .setValue(summarizable.min())
            )
            .addAllQuantileValues(
                Arrays.stream(QUANTILES).mapToObj(
                    q -> ValueAtQuantile.newBuilder()
                        .setQuantile(q)
                        .setValue(yammerSnapshot.getValue(q))
                        .build()
                )::iterator
            )
            .addQuantileValues(
                ValueAtQuantile.newBuilder().setQuantile(1)
                    .setValue(summarizable.max())
            )
            .build();

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
            .setName(metricKey.name())
            .setUnit(unit);

        metric.getSummaryBuilder()
            .addDataPoints(summary)
            .build();

        return SinglePointMetric.create(metricKey, metric);
    }

    private SinglePointMetric histogram1(
        MetricKey metricKey,
        String unit,
        Snapshot yammerSnapshot,
        long count,
        Summarizable summarizable) {
        HistogramDataPoint histogramDataPoint = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(MetricsUtils.toTimeUnixNanos(clock.instant()))
            .addAllAttributes(SinglePointMetric.asAttributes(metricKey.tags()))
            .setCount(count)
            .setSum(summarizable.sum())
//            .setMin(summarizable.min())
//            .setMax(summarizable.max())
            .addAllAttributes(Arrays.stream(QUANTILES).mapToObj(
                    q -> KeyValue.newBuilder()
                        .setKey(String.valueOf(q))
                        .setValue(AnyValue.newBuilder().setDoubleValue(yammerSnapshot.getValue(q)).build())
                        .build()
                )::iterator
            )
            .build();

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
            .setName(metricKey.name())
            .setUnit(unit);

        metric.getHistogramBuilder()
            .addDataPoints(histogramDataPoint)
            .build();

        return SinglePointMetric.create(metricKey, metric);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private MetricsRegistry metricsRegistry;
        private MetricNamingStrategy<MetricName> metricNamingStrategy;
        private Clock clock = Clock.systemUTC();
        private LastValueTracker<Long> longDeltas = new LastValueTracker<>();
        private LastValueTracker<Double> doubleDeltas = new LastValueTracker<>();

        private Builder() {
        }

        public Builder setMetricsRegistry(MetricsRegistry metricsRegistry) {
            this.metricsRegistry = metricsRegistry;
            return this;
        }

        public Builder setMetricNamingStrategy(MetricNamingStrategy<MetricName> metricNamingStrategy) {
            this.metricNamingStrategy = metricNamingStrategy;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = Objects.requireNonNull(clock);
            return this;
        }

        public YammerMetricsCollector build() {
            Objects.requireNonNull(this.metricNamingStrategy);
            Objects.requireNonNull(this.metricsRegistry);

            return new YammerMetricsCollector(this.metricsRegistry, this.metricNamingStrategy, this.longDeltas, this.doubleDeltas, this.clock);
        }

        public Builder setLongDeltas(LastValueTracker<Long> longDeltas) {
            this.longDeltas = longDeltas;
            return this;
        }

        public Builder setDoubleDeltas(LastValueTracker<Double> doubleDeltas) {
            this.doubleDeltas = doubleDeltas;
            return this;
        }

    }
}
