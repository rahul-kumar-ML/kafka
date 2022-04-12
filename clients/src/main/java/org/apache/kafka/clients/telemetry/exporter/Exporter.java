package org.apache.kafka.clients.telemetry.exporter;

import org.apache.kafka.clients.telemetry.metrics.Keyed;
import org.apache.kafka.clients.telemetry.metrics.SerializedMetric;
import org.apache.kafka.common.metrics.Metrics;

import java.util.function.Predicate;

// A client is responsible for sending metrics correctly to backend
public interface Exporter extends AutoCloseable {

    /**
     * Reconfigure the metrics predicate.
     * @param metricsPredicate metrics predicate to switch to
     */
    void reconfigurePredicate(Predicate<? super Keyed> metricsPredicate);

    /*
     * Export the metric to the destination. This method takes care
     * of batching, serialization and retries.
     */
    boolean emit(SerializedMetric metric);

    /**
     * Set the Metrics registry on this Exporter.
     * The Exporter can use this to register its own metrics.
     */
    default void setMetricsRegistry(Metrics metrics) { }
}
