package org.apache.kafka.clients.telemetry;

import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.resource.v1.Resource;

/**
 * Context for metrics collectors.
 *
 * Encapsulates metadata such as the OpenCensus {@link Resource} and
 * includes utility methods for constructing {@link Metric}s that automatically
 * attach the <code>Resource</code> to the <code>Metric</code>.
 */
public class Context {

    private final Resource resource;

    private final boolean debugEnabled;

    private final String domain;

    public Context(String domain) {
        this(Resource.getDefaultInstance(), domain);
    }

    public Context(Resource resource, String domain) {
        this(resource, domain, false);
    }

    public Context(Resource resource, String domain, boolean debugEnabled) {
        this.resource = resource;
        this.debugEnabled = debugEnabled;
        this.domain = domain;
    }

    /**
     * Get the {@link Resource} in this Context.
     * The <code>Resource</code> represents the entity for which telemetry is being collected.
     *
     * <p>
     * The <code>Resource</code> <b>must</b> be set on every <code>Metric</code> collected and reported.
     * This can be enforced by using the {@link #buildMetric(MetricBuilderFacade)} ()} method to construct <code>Metrics</code>.
     */
    public Resource getResource() {
        return resource;
    }

    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    public String getDomain() {
        return this.domain;
    }

    /**
     * Set the context for {@link MetricBuilderFacade} and build a {@link Metric}
     */
    public Metric buildMetric(MetricBuilderFacade builder) {
        return builder.withResource(resource).build();
    }

}
