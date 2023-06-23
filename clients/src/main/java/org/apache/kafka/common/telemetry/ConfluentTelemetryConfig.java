// (Copyright) [2016 - 2016] Confluent, Inc.

package org.apache.kafka.common.telemetry;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentTelemetryConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(ConfluentTelemetryConfig.class);

    public static final String PREFIX = "confluent.telemetry.";
    public static final String PREFIX_LABELS = PREFIX + "labels.";
    public static final String PREFIX_EXPORTER = PREFIX + "exporter.";
    public static final String PREFIX_METRICS_COLLECTOR = PREFIX + "metrics.collector.";

    public static final String COLLECT_INTERVAL_CONFIG = PREFIX_METRICS_COLLECTOR + "interval.ms";
    public static final Long DEFAULT_COLLECT_INTERVAL = TimeUnit.MINUTES.toMillis(1);
    public static final String COLLECT_INTERVAL_DOC = "The metrics reporter will collect new metrics "
            + "from the system in intervals defined by this setting. This means that control "
            + "center system health data lags by this duration, or that rebalancer may compute a plan "
            + "based on broker data that is stale by this duration. The default is a reasonable value "
            + "for production environments and it typically does not need to be changed.";

    public static final String METRICS_INCLUDE_CONFIG = PREFIX_METRICS_COLLECTOR + "include";
    public static final String METRICS_INCLUDE_CONFIG_ALIAS = PREFIX_METRICS_COLLECTOR + "whitelist";

    public static final String SLO_COLLECTOR_ENABLED = PREFIX_METRICS_COLLECTOR + "slo.enabled";
    public static final Boolean DEFAULT_SLO_COLLECTOR_ENABLED = false;
    public static final String SLO_COLLECTOR_DOC = "Enable SLO metric collector";

    public static final String DEFAULT_SYSTEM_METRICS_INCLUDE_REGEX =
        "io.confluent.system/.*("
            + "process_cpu_load"
            + "|max_file_descriptor_count"
            + "|open_file_descriptor_count"
            + "|system_cpu_load"
            + "|system_load_average"
            + "|free_physical_memory_size"
            + "|total_physical_memory_size"
            + "|disk_total_bytes"
            + "|disk_usable_bytes"
            + "|jvm/mem"
            + "|jvm/gc"
            + ")";

    public static final String METRICS_INCLUDE_DOC =
        "Regex matching the converted (snake_case) metric name to be published to the "
        + "metrics topic.\n\nBy default this includes all the metrics required by "
        + "Proactive Support and Self-balancing Clusters. This should typically never "
        + "be modified unless requested by Confluent.";
    public static final String DEFAULT_METRICS_INCLUDE;

    public static String joinIncludeRegexList(List<String> includeRegexList) {
        StringBuilder eventBuilder = new StringBuilder(".*");
        Joiner.on(".*|.*").appendTo(eventBuilder, includeRegexList);
        eventBuilder.append(".*");
        return eventBuilder.toString();
    }

    static {
        DEFAULT_METRICS_INCLUDE = joinIncludeRegexList(Collections.singletonList(
            DEFAULT_SYSTEM_METRICS_INCLUDE_REGEX));
    }

    public static final String DEBUG_ENABLED = PREFIX + "debug.enabled";
    public static final String DEBUG_ENABLED_DOC = "Enable debug metadata for metrics collection";
    public static final boolean DEFAULT_DEBUG_ENABLED = false;

    // Name of the default NamedFilter. The default NamedFilter include list is populated
    // using the METRICS_INCLUDE_CONFIG value. This value can be updated dynamically
    public static final String DEFAULT_NAMED_FILTER_NAME = "_default";
    public static final Set<String> DEFAULT_ACTIVE_FILTER_SET = Collections.singleton(DEFAULT_NAMED_FILTER_NAME);
    /*
     * This is same code as in LinuxCpuMetricsCollector, duplicated because of Scala/Java
     * compatibility.
     */
    public static final String CPU_METRIC;
    static {
        String procRoot = "/proc";
        Path loadAvgPath = Paths.get(procRoot, "loadavg");
        Path statPath = Paths.get(procRoot, "stat");
        boolean isLinuxCpuCollectorUsable = loadAvgPath.toFile().exists() && statPath.toFile().exists();
        log.info("Linux CPU collector enabled: {}", isLinuxCpuCollectorUsable);
        // If linux collector is not enabled (say we are running on Mac/Windows), then switch to metric
        // collected by JVM
        CPU_METRIC = isLinuxCpuCollectorUsable ? "io\\.confluent\\.kafka\\.server/server/linux_system_cpu_utilization_1m" :
                "io\\.confluent\\.system/jvm/os/process_cpu_load";
        log.info("Using cpu metric: {}", CPU_METRIC);
    }

    public static String exporterPrefixForName(String name) {
        return PREFIX_EXPORTER + name + ".";
    }

    public static final String TELEMETRY_ENABLED_CONFIG = PREFIX + "enabled";
    public static final String TELEMETRY_ENABLED_DOC = "True if telemetry data can to be reported to Confluent Cloud";
    public static final boolean TELEMETRY_ENABLED_DEFAULT = false;

    public static final Set<String> RECONFIGURABLES =
        ImmutableSet.of(
            // not including METRICS_INCLUDE_CONFIG_ALIAS in dynamic configs,
            // since we have never set those configs dynamically anywhere
            METRICS_INCLUDE_CONFIG,
            TELEMETRY_ENABLED_CONFIG
        );

    // Internal map used to reconcile config parameters for default _confluent http exporter using the
    // high level end-user friendly http.telemetry flags and http.telemetry.exporter._confluent flags.
    // Sample Map contents:
    // Map(
    //   confluent.telemetry.enabled => confluent.telemetry.exporter._confluent.enabled,
    //   confluent.telemetry.api.key => confluent.telemetry.exporter._confluent.api.key,
    //   confluent.telemetry.api.secret => confluent.telemetry.exporter._confluent.api.secret,
    //   confluent.telemetry.proxy.url => confluent.telemetry.exporter._confluent.proxy.url
    //   confluent.telemetry.proxy.username => confluent.telemetry.exporter._confluent.proxy.username
    //   confluent.telemetry.proxy.password => confluent.telemetry.exporter._confluent.proxy.password
    //)

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(
                COLLECT_INTERVAL_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_COLLECT_INTERVAL,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.LOW,
                COLLECT_INTERVAL_DOC
        ).define(
                METRICS_INCLUDE_CONFIG,
                ConfigDef.Type.STRING,
                DEFAULT_METRICS_INCLUDE,
                new RegexConfigDefValidator("Metrics filter for configuration"),
                ConfigDef.Importance.LOW,
                METRICS_INCLUDE_DOC
        ).define(
                SLO_COLLECTOR_ENABLED,
                ConfigDef.Type.BOOLEAN,
                DEFAULT_SLO_COLLECTOR_ENABLED,
                ConfigDef.Importance.LOW,
                SLO_COLLECTOR_DOC
        ).define(
                DEBUG_ENABLED,
                ConfigDef.Type.BOOLEAN,
                DEFAULT_DEBUG_ENABLED,
                ConfigDef.Importance.LOW,
                DEBUG_ENABLED_DOC
        ).define(
            TELEMETRY_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            TELEMETRY_ENABLED_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            TELEMETRY_ENABLED_DOC
        );

    public ConfluentTelemetryConfig(Map<String, ?> originals) {
        this(originals, true);
    }

    public ConfluentTelemetryConfig(Map<String, ?> originals, boolean doLog) {
        super(CONFIG, ConfigUtils.translateDeprecated(originals, new String[][]{
            {METRICS_INCLUDE_CONFIG, METRICS_INCLUDE_CONFIG_ALIAS}}), doLog);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toRst());
    }

    public Map<String, String> getLabels() {
        Map<String, String> labels = new HashMap<>();
        for (Map.Entry<String, ?> entry : super.originals().entrySet()) {
            if (entry.getKey().startsWith(PREFIX_LABELS)) {
                labels.put(entry.getKey().substring(PREFIX_LABELS.length()), (String) entry.getValue());
            }
        }
        return labels;
    }
}
