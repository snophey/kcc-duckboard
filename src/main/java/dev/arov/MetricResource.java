package dev.arov;

import io.quarkus.logging.Log;
import io.quarkus.runtime.configuration.ConfigUtils;
import io.smallrye.common.annotation.Blocking;
import io.spoud.kcc.data.AggregatedDataWindowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestPath;
import org.jboss.resteasy.reactive.RestQuery;

import java.util.List;
import java.util.Set;

@Produces(MediaType.APPLICATION_JSON)
@Path("/api/metrics")
@Blocking
public class MetricResource {
    @Inject
    MetricRepository metricRepository;

    @GET
    public Set<String> getMetrics() {
        return metricRepository.getAllMetrics();
    }

    @GET
    @Path("/{metricName}/{aggType}")
    public List<AggregatedDataWindowed> getAggregatedMetric(@RestPath String metricName,
                                                            @RestPath MetricRepository.AggregationType aggType,
                                                            @RestQuery MetricRepository.FilterSpec filter,
                                                            @RestQuery MetricRepository.TimestampParam startTimestamp,
                                                            @RestQuery MetricRepository.TimestampParam endTimestamp,
                                                            @RestQuery MetricRepository.GroupBySpec groupBy,
                                                            @RestQuery Integer limit,
                                                            @RestQuery Integer offset,
                                                            @RestQuery MetricRepository.SortOrder sort) {
        Log.infof("All params: metric=%s, aggType=%s, filter=%s, start=%s, end=%s, groupBy=%s, limit=%s, offset=%s, sort=%s",
                metricName, aggType, filter, startTimestamp, endTimestamp, groupBy, limit, offset, sort);
        return metricRepository.getAggregatedMetric(metricName,
                aggType,
                filter == null ? new MetricRepository.FilterSpec(List.of(), List.of()) : filter,
                startTimestamp,
                endTimestamp,
                groupBy == null ? new MetricRepository.GroupBySpec(List.of(), List.of(), false, false, false) : groupBy,
                limit != null && limit < 0 ? null : limit,
                offset != null && offset < 0 ? null : offset,
                sort);
    }

    @GET
    @Path("/tags")
    public Set<String> getTags() {
        return metricRepository.getAllTagKeys();
    }

    @GET
    @Path("/tags/{tagKey}/values")
    public Set<String> getTagValues(@RestPath String tagKey) {
        return metricRepository.getAllTagValues(tagKey);
    }

    @GET
    @Path("/contexts")
    public Set<String> getContexts() {
        return metricRepository.getAllContextKeys();
    }

    @GET
    @Path("/contexts/{contextKey}/values")
    public Set<String> getContextValues(@RestPath String contextKey) {
        return metricRepository.getAllContextValues(contextKey);
    }

    // only for testing purposes (disable in production)
    @POST
    @Path("/query")
    @Consumes(MediaType.TEXT_PLAIN)
    public String runQuery(String query) {
        if (!ConfigUtils.getProfiles().contains("test") && !ConfigUtils.getProfiles().contains("dev")) {
            Log.warn("Someone tried to run the following query in production: " + query);
            Log.warn("If this was you, remember that this endpoint is only available if QUARKUS_PROFILE=test or dev. Current profiles: " + ConfigUtils.getProfiles());
            throw new NotFoundException();
        }
        return metricRepository.runQuery(query);
    }
}
