package org.apache.kafka.common.telemetry;

import com.google.common.collect.Maps;
import java.util.Map;

public class ExporterUtils {
    //Parse exporter configs, add result to a map of exporter configs, key is exporter name, value is a map of exporter configs of the exporter
    public static void parseExporterConfigsByName(Map<String, Map<String, Object>> exporterConfigs, Map<String, Object> configs) {
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            String key = entry.getKey();
            int nameLength = key.indexOf(".");
            if (nameLength > 0) {
                String name = key.substring(0, nameLength);
                //stripped off "{exporter_name}." prefix
                String property = key.substring(nameLength + 1);
                exporterConfigs
                        .computeIfAbsent(name, k -> Maps.newHashMap())
                        .put(property, entry.getValue());
            }
        }
    }
}
