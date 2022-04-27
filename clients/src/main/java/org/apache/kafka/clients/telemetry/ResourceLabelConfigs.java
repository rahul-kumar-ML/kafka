package org.apache.kafka.clients.telemetry;

import org.apache.kafka.clients.CommonClientConfigs;

public class ResourceLabelConfigs {
    public static final String RACK_ID = CommonClientConfigs.CLIENT_RACK_CONFIG;

    // Consumer resource labels
    public static final String CONSUMER_GROUP_ID = "group.id";
    public static final String CONSUMER_GROUP_MEMBERSHIP_ID = "group.member.id";
    public static final String CONSUMER_GROUP_INSTASNCE_ID = "group.instance.id";

    // Producer
    public static final String PRODUCER_TRANSACTIONAL_ID = "transactional.id";

    // Stream
    public static final String STREAM_APPLICATION_ID = "application.id";
}
