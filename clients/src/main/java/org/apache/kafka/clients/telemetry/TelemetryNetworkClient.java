package org.apache.kafka.clients.telemetry;

import static org.apache.kafka.common.Uuid.ZERO_UUID;

import java.util.List;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryNetworkClient {

    private static final Logger log = LoggerFactory.getLogger(TelemetryNetworkClient.class);

    private final TelemetryManagementInterface tmi;

    public TelemetryNetworkClient(TelemetryManagementInterface tmi) {
        this.tmi = tmi;
        tmi.setState(TelemetryState.subscription_needed);
    }

    public void telemetrySubscriptionFailed(Throwable error) {
        if (error != null)
            log.warn("Failed to retrieve telemetry subscription; using existing subscription", error);
        else
            log.warn("Failed to retrieve telemetry subscription; using existing subscription");

        tmi.setState(TelemetryState.subscription_needed);
    }

    public void pushTelemetryFailed(Exception error) {
        if (error != null)
            log.warn("Failed to push telemetry", error);
        else
            log.warn("Failed to push telemetry", new Exception());

        TelemetryState state = tmi.state();

        if (state == TelemetryState.push_in_progress)
            tmi.setState(TelemetryState.subscription_needed);
        else if (state == TelemetryState.terminating_push_in_progress)
            tmi.setState(TelemetryState.terminated);
        else
            throw new IllegalTelemetryStateException(String.format("Could not transition state after failed push telemetry from state %s", state));
    }

    public void telemetrySubscriptionSucceeded(GetTelemetrySubscriptionsResponseData data) {
        Time time = tmi.time();
        Set<MetricName> metricNames = TelemetryUtils.validateMetricNames(data.requestedMetrics());
        List<CompressionType> acceptedCompressionTypes = TelemetryUtils.validateAcceptedCompressionTypes(data.acceptedCompressionTypes());
        Uuid clientInstanceId = TelemetryUtils.validateClientInstanceId(data.clientInstanceId());
        int pushIntervalMs = TelemetryUtils.validatePushInteravlMs(data.pushIntervalMs());

        TelemetrySubscription telemetrySubscription = new TelemetrySubscription(time.milliseconds(),
            data.throttleTimeMs(),
            clientInstanceId,
            data.subscriptionId(),
            acceptedCompressionTypes,
            pushIntervalMs,
            data.deltaTemporality(),
            metricNames);

        log.debug("Successfully retrieved telemetry subscription: {}", telemetrySubscription);

        tmi.setSubscription(telemetrySubscription);
        tmi.setState(TelemetryState.push_needed);
    }

    public void pushTelemetrySucceeded() {
        log.trace("Successfully pushed telemetry");

        TelemetryState state = tmi.state();

        if (state == TelemetryState.push_in_progress)
            tmi.setState(TelemetryState.subscription_needed);
        else if (state == TelemetryState.terminating_push_in_progress)
            tmi.setState(TelemetryState.terminated);
        else
            throw new IllegalTelemetryStateException(String.format("Could not transition state after successful push telemetry from state %s", state));
    }

    public boolean isNetworkState() {
        TelemetryState s = tmi.state();
        return s == TelemetryState.subscription_in_progress || s == TelemetryState.push_in_progress || s == TelemetryState.terminating_push_in_progress;
    }

    public boolean isTerminatedState() {
        TelemetryState s = tmi.state();
        return s == TelemetryState.terminated;
    }

    public long timeToNextUpdate() {
        long t = 0;
        TelemetryState s = tmi.state();

        if (s == TelemetryState.initialized || s == TelemetryState.subscription_needed) {
            // TODO: TELEMETRY_TODO: verify
            t = 0;
        } else  if (s == TelemetryState.terminated) {
            // TODO: TELEMETRY_TODO: verify and add a good error message
            throw new IllegalTelemetryStateException();
        } else {
            long milliseconds = tmi.time().milliseconds();
            TelemetrySubscription subscription = tmi.subscription();

            if (subscription != null) {
                long fetchMs = subscription.fetchMs();
                long pushIntervalMs = subscription.pushIntervalMs();
                t = fetchMs + pushIntervalMs - milliseconds;

                if (t < 0)
                    t = 0;
            }
        }

        log.debug("For state {}, returning {} for time to next update", s, t);
        return t;
    }

    public AbstractRequest.Builder<?> createRequest() {
        TelemetrySubscription subscription = tmi.subscription();

        if (tmi.state() == TelemetryState.subscription_needed) {
            tmi.setState(TelemetryState.subscription_in_progress);
            Uuid clientInstanceId = subscription != null ? subscription.clientInstanceId() : ZERO_UUID;
            return new GetTelemetrySubscriptionRequest.Builder(clientInstanceId);
        } else if (tmi.state() == TelemetryState.push_needed) {
            if (subscription == null)
                throw new IllegalStateException(String.format("Telemetry state is %s but subscription is null", tmi.state()));

            boolean terminating = tmi.state() == TelemetryState.terminating;
            CompressionType compressionType = TelemetryUtils.preferredCompressionType(subscription.acceptedCompressionTypes());
            Bytes metricsData = TelemetryUtils.collectMetricsPayload(tmi, compressionType, subscription.deltaTemporality());

            if (terminating)
                tmi.setState(TelemetryState.terminating_push_in_progress);
            else
                tmi.setState(TelemetryState.push_in_progress);

            return new PushTelemetryRequest.Builder(subscription.clientInstanceId(),
                subscription.subscriptionId(),
                terminating,
                compressionType,
                metricsData);
        } else {
            throw new IllegalTelemetryStateException(String.format("Cannot make telemetry request as telemetry is in state: %s", tmi.state()));
        }
    }

}
