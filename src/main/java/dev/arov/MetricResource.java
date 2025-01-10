package dev.arov;

import io.smallrye.common.annotation.Blocking;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestPath;

@Produces(MediaType.APPLICATION_JSON)
@Path("/metrics")
public class MetricResource {
    @Inject
    MetricRepository metricRepository;

    @Blocking //  we are talking to the database, which could take a while
    @GET
    @Path("/average/{metricName}")
    public double getMetric(@RestPath String metricName) {
        return metricRepository.getMetricAverage(metricName);
    }
}
