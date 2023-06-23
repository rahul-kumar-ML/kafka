package org.apache.kafka.common.telemetry;

import com.google.common.base.Verify;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.telemetry.collector.MetricsCollector;
import org.apache.kafka.common.telemetry.emitter.Emitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsCollectorTask {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollectorTask.class);

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    private final Collection<MetricsCollector> collectors;
    private final long collectIntervalMs;
    private final Emitter emitter;

    public MetricsCollectorTask(Collection<MetricsCollector> collectors,
                                long collectIntervalMs,
                                Emitter emitter
    ) {
        Verify.verify(collectIntervalMs > 0, "collection interval cannot be less than 1");

        this.collectors = Objects.requireNonNull(collectors);
        this.collectIntervalMs = collectIntervalMs;
        this.emitter = emitter;

        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setThreadFactory(runnable -> {
            Thread thread = new Thread(runnable, "confluent-telemetry-metrics-collector-task-scheduler");
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}':", t.getName(), e));
            return thread;
        });
    }

    public void start() {
        log.info("[APM] - start");
        schedule();
    }

    private void schedule() {
        log.info("[APM] - schedule");
        executor.scheduleAtFixedRate(
                this::collectAndExport,
                collectIntervalMs,
                collectIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    private void collectAndExport() {
        log.info("[APM] - collectAndExport1");
        collectors.forEach(this::collectAndExport);
    }

    private void collectAndExport(MetricsCollector collector) {
        log.info("[APM] - collectAndExport");
        try {
            collector.collect(emitter);
        } catch (Throwable t) {
            log.error("Error while collecting metrics for collector = {})",
                collector,
                t);
        }
    }

    public void close() {
        executor.shutdown();
    }

}
