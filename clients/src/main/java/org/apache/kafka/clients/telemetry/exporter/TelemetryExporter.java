package org.apache.kafka.clients.telemetry.exporter;

import org.apache.kafka.clients.telemetry.metrics.Keyed;
import org.apache.kafka.clients.telemetry.metrics.SerializedMetric;

import java.util.function.Predicate;

public class TelemetryExporter implements Exporter {
    @Override
    public void reconfigurePredicate(Predicate<? super Keyed> metricsPredicate) {

    }

    @Override
    public boolean emit(SerializedMetric metric) {
        return false;
    }

    @Override
    public void close() throws Exception {

    }
}
