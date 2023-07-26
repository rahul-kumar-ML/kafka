package org.apache.kafka.common.telemetry;


import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MetricsUtils {

    public static long toTimeUnixNanos(Instant t) {
        return TimeUnit.SECONDS.toNanos(t.getEpochSecond()) + t.getNano();
    }

    public static ZonedDateTime nowInUTC(Clock clock) {
        return Instant.now(clock).atZone(ZoneOffset.UTC);
    }

    public static OffsetDateTime nowInUTC() {
        return OffsetDateTime.now(Clock.systemUTC());
    }

    /**
     * Convert resource attributes to a map of strings, ignoring non-string attributes
     * @param resource
     * @return
     */
    public static Map<String, String> attributesMap(Resource resource) {
        return attributesMap(resource.getAttributesList());
    }

    public static Map<String, String> attributesMap(List<KeyValue> attributes) {
        return attributes.stream()
            .filter(attr -> attr.getValue().hasStringValue())
            .collect(Collectors.toMap(
                KeyValue::getKey,
                attr -> attr.getValue().getStringValue()
            ));
    }
}
