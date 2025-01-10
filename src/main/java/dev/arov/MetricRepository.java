package dev.arov;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.spoud.kcc.data.AggregatedDataWindowed;
import jakarta.enterprise.event.Observes;
import jakarta.ws.rs.BadRequestException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBTimestamp;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.*;

import jakarta.enterprise.context.ApplicationScoped;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@ApplicationScoped
public class MetricRepository {
    private DuckDBConnection connection;
    private static final String TABLE_NAME = "aggregated_data";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @ConfigProperty(name = "seed.with.fake.data")
    boolean seedWithFakeData;


    void onStartup(@Observes StartupEvent ev) {
        // this is where we create DuckDb tables
        Log.info("Initializing OLAP database");
        getConnection().ifPresent((conn) -> {
            try {
                createTableIfNotExists(conn);
            } catch (SQLException e) {
                Log.error("Failed to create table", e);
            }
        });
        if (seedWithFakeData) {
            Log.info("Seeding database with fake data");
            var faker = new Faker();
            var allAppNames = IntStream.range(0, 10)
                    .mapToObj(i -> faker.app().name())
                    .toArray(String[]::new);
            var allCountries = IntStream.range(0, 4)
                    .mapToObj(i -> faker.country().countryCode2())
                    .toArray(String[]::new);
            var metrics = IntStream.range(0, 1000_000)
                    .mapToObj(i -> {
                        var start = Instant.now().minus(Duration.ofDays(faker.number().numberBetween(1, 365))).minus(Duration.ofHours(faker.number().numberBetween(1, 24)));
                        var appName = faker.options().option(allAppNames);
                        return AggregatedDataWindowed.newBuilder()
                                .setStartTime(start)
                                .setEndTime(start.plus(Duration.ofDays(1)))
                                .setInitialMetricName(faker.options().option("bytesin", "bytesout", "storage"))
                                .setName(faker.animal().name())
                                .setTags(Map.of("region", faker.options().option(allCountries)))
                                .setContext(Map.of("app", appName))
                                .setValue(faker.number().randomDouble(0, 100, 1000_000))
                                .build();
                    })
                    .toList();
            ingestMetrics(metrics, Function.identity());
        }
    }

    private void createTableIfNotExists(DuckDBConnection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                    "start_time TIMESTAMP_MS NOT NULL, " +
                    "end_time TIMESTAMP_MS NOT NULL, " +
                    "initial_metric_name VARCHAR NOT NULL, " +
                    "name VARCHAR NOT NULL, " +
                    "tags JSON NOT NULL, " +
                    "context JSON NOT NULL, " +
                    "value DOUBLE NOT NULL)");
        }
    }

    /**
     * Consume the message from the "words-in" channel, uppercase it and send it to the uppercase channel.
     * Messages come from the broker.
     **/
    @Incoming("words-in")
    public void onIncomingMessages(ConsumerRecords<String, AggregatedDataWindowed> messages) {
        Log.infof("Received batch of %d metrics", messages.count());
        ingestMetrics(messages, ConsumerRecord::value);
    }

    private Optional<DuckDBConnection> getConnection() {
        try {
            if (connection == null) {
                connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
            }
        } catch (Exception e) {
            Log.error("Failed to get read-write connection to OLAP database", e);
        }
        return Optional.ofNullable(connection);
    }

    private <W> void ingestMetrics(Iterable<W> wrappedMetrics, Function<W, AggregatedDataWindowed> unwrapFunction) {
        getConnection().ifPresent((conn) -> {
            var skipped = 0;
            try (var appender = conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, TABLE_NAME)) {
                for (var wrappedMetric : wrappedMetrics) {
                    var metric = unwrapFunction.apply(wrappedMetric);
                    Log.debugv("Ingesting metric: {0}", metric);
                    var start = DuckDBTimestamp.fromMilliInstant(metric.getStartTime().toEpochMilli());
                    var end = DuckDBTimestamp.fromMilliInstant(metric.getEndTime().toEpochMilli());
                    var tags = "";
                    var context = "";
                    try {
                        tags = OBJECT_MAPPER.writeValueAsString(metric.getTags());
                        context = OBJECT_MAPPER.writeValueAsString(metric.getContext());
                    } catch (JsonProcessingException e) {
                        Log.warn("Failed to serialize tags or context. Skipping metric...", e);
                        skipped++;
                        continue;
                    }
                    appender.beginRow();
                    appender.appendLocalDateTime(start.toLocalDateTime());
                    appender.appendLocalDateTime(end.toLocalDateTime());
                    appender.append(metric.getInitialMetricName());
                    appender.append(metric.getName());
                    appender.append(tags);
                    appender.append(context);
                    appender.append(metric.getValue());
                    appender.endRow();
                }
            } catch (SQLException e) {
                Log.error("Failed to ingest ALL metrics to OLAP database", e);
                return;
            }
            Log.infof("Ingestion complete. Skipped %d metrics.", skipped);
        });
    }

    public Set<String> getAllTagKeys() {
        return getAllJsonKeys("tags");
    }

    public Set<String> getAllContextKeys() {
        return getAllJsonKeys("context");
    }

    private Set<String> getAllJsonKeys(String column) {
        return getConnection()
                .map(conn -> {
                    try (var statement = conn.prepareStatement("SELECT DISTINCT json_keys( " + column + " ) FROM " + TABLE_NAME)) {
                        var result = statement.executeQuery();
                        var keys = new HashSet<String>();
                        while (result.next()) {
                            var key = result.getString(1);
                            if (key != null) {
                                keys.add(key.substring(1, key.length() - 1)); // remove brackets
                            }
                        }
                        return keys;
                    } catch (Exception e) {
                        Log.error("Failed to get keys of column: " + column, e);
                    }
                    return new HashSet<String>();
                })
                .orElse(new HashSet<>());
    }

    public List<AggregatedDataWindowed> getAggregatedMetric(String metricName,
                                                      AggregationType aggType,
                                                      FilterSpec tagFilter,
                                                      TimestampParam startTimestamp,
                                                      TimestampParam endTimestamp,
                                                      GroupBySpec groupBy,
                                                      Integer limit,
                                                      Integer offset,
                                                      SortOrder sort) {
        return getConnection()
                .map(conn -> {
                    try {
                        var params = new ArrayList<>();
                        var query = new StringBuilder("SELECT ");

                        var selected = Stream.concat(
                                groupBy.contexts().stream().map(ctx -> String.format("COALESCE(context->>'%s', 'unknown') as \"%s\"", ctx, "ctx:" + ctx)),
                                groupBy.tags().stream().map(ctx -> String.format("COALESCE(tags->>'%s', 'unknown') as \"%s\"", ctx, "tag:" + ctx))
                        ).collect(Collectors.joining(", ", " ", " "));
                        if (!selected.isBlank()) {
                            query.append(selected).append(", ");
                        }
                        if (groupBy.groupByResourceName()) {
                            query.append("name, ");
                        }

                        switch (aggType) {
                            case AVG -> query.append("AVG(value) as value");
                            case SUM -> query.append("SUM(value) as value");
                            case COUNT -> query.append("COUNT(value) as value");
                            case MIN -> query.append("MIN(value) as value");
                            case MAX -> query.append("MAX(value) as value");
                        }
                        query.append(" FROM ").append(TABLE_NAME).append(" WHERE initial_metric_name = ?");
                        params.add(metricName);
                        if (startTimestamp != null) {
                            query.append(" AND start_time >= ?");
                            params.add(Timestamp.valueOf(startTimestamp.timestamp()));
                        }
                        if (endTimestamp != null) {
                            query.append(" AND end_time <= ?");
                            params.add(Timestamp.valueOf(endTimestamp.timestamp()));
                        }
                        for (var filter : tagFilter.tagFilters()) {
                            query.append(String.format(" AND (tags->>'%s' = ?)", filter.key()));
                            params.add(filter.value());
                        }
                        for (var filter : tagFilter.contextFilters()) {
                            query.append(String.format(" AND (context->>'%s' = ?)", filter.key()));
                            params.add(filter.value());
                        }
                        if (groupBy.groupByResourceName() || (!groupBy.tags().isEmpty() || !groupBy.contexts().isEmpty())) {
                            query.append(" GROUP BY ");
                            query.append(groupBy.tags().stream().map(k -> String.format("\"tag:%s\"", k)).collect(Collectors.joining(", ")));
                            if (!groupBy.tags().isEmpty() && !groupBy.contexts().isEmpty())
                                query.append(", ");
                            query.append(groupBy.contexts().stream().map(k -> String.format("\"ctx:%s\"", k)).collect(Collectors.joining(", ")));
                            if (groupBy.groupByResourceName()) {
                                query.append(", name");
                            }
                        }
                        if (sort != null) {
                            query.append(" ORDER BY value ");
                            query.append(sort);
                        }
                        if (limit != null) {
                            query.append(" LIMIT ?");
                            params.add(limit);
                        }
                        if (offset != null) {
                            query.append(" OFFSET ?");
                            params.add(offset);
                        }
                        try (var statement = conn.prepareStatement(query.toString())) {
                            for (int i = 0; i < params.size(); i++) {
                                statement.setObject(i + 1, params.get(i));
                            }
                            var result = statement.executeQuery();
                            var metrics = new ArrayList<AggregatedDataWindowed>();
                            var startTime = startTimestamp != null ? startTimestamp.timestamp().atZone(ZoneId.systemDefault()).toInstant() : Instant.EPOCH;
                            var endTime = endTimestamp != null ? endTimestamp.timestamp().atZone(ZoneId.systemDefault()).toInstant() : Instant.now();
                            while (result.next()) {
                                var aggValue = result.getDouble("value");
                                var name = groupBy.groupByResourceName() ? result.getString("name") : "unknown";
                                var tags = new HashMap<String, String>();
                                var context = new HashMap<String, String>();
                                for (var tag : groupBy.tags()) {
                                    tags.put(tag, result.getString("tag:" + tag));
                                }
                                for (var ctx : groupBy.contexts()) {
                                    context.put(ctx, result.getString("ctx:" + ctx));
                                }
                                metrics.add(AggregatedDataWindowed.newBuilder()
                                                .setContext(context)
                                                .setTags(tags)
                                                .setInitialMetricName(metricName)
                                                .setName(name)
                                                .setStartTime(startTime)
                                                .setEndTime(endTime)
                                                .setValue(aggValue)
                                        .build());
                            }
                            return metrics;
                        }
                    } catch (SQLException e) {
                        Log.error("Failed to get aggregated metric", e);
                    }
                    return null;
                })
                .orElse(null);
    }


    // only for debugging purposes
    public String runQuery(String query) {
        return getConnection()
                .map(conn -> {
                    try (var statement = conn.prepareStatement(query)) {
                        var result = statement.executeQuery();
                        var rows = new ArrayList<Map<String, Object>>();
                        while (result.next()) {
                            var row = new HashMap<String, Object>();
                            for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                                row.put(result.getMetaData().getColumnName(i), result.getObject(i));
                            }
                            rows.add(row);
                        }
                        return OBJECT_MAPPER.writeValueAsString(rows);
                    } catch (SQLException | JsonProcessingException e) {
                        Log.error("Failed to run query", e);
                    }
                    return "";
                })
                .orElse("");
    }

    public record GroupBySpec(List<String> tags, List<String> contexts, boolean groupByResourceName) {
        public static GroupBySpec fromString(String value) {
            var parts = value.split(",");
            var tags = new ArrayList<String>();
            var contexts = new ArrayList<String>();
            var groupByResourceName = false;
            for (var part : parts) {
                if (part.startsWith("tag:")) {
                    var tagKey = part.substring(4);
                    ensureIdentifierIsSafe(tagKey);
                    if (!tagKey.isBlank()) {
                        tags.add(tagKey);
                    }
                } else if (part.startsWith("context:")) {
                    var contextKey = part.substring(8);
                    ensureIdentifierIsSafe(contextKey);
                    if (!contextKey.isBlank()) {
                        contexts.add(contextKey);
                    }
                } else if (part.startsWith("builtin:")) {
                    var builtinKey = part.substring(8);
                    groupByResourceName = builtinKey.equals("resourceName");
                }
            }
            return new GroupBySpec(tags, contexts, groupByResourceName);
        }
    }

    public record FilterPair(String key, String value) {}

    public record FilterSpec(List<FilterPair> tagFilters, List<FilterPair> contextFilters) {
        // a filter spec is a comma separated list of the form type:key:value, where type is either "tag" or "context"
        public static FilterSpec fromString(String value) {
            var parts = value.split(",");
            var tagFilters = new ArrayList<FilterPair>();
            var contextFilters = new ArrayList<FilterPair>();
            for (var part : parts) {
                try {
                    var split = part.split(":");
                    var type = split[0];
                    var key = split[1];
                    var val = split[2];
                    ensureIdentifierIsSafe(key);
                    if (type.equals("tag")) {
                        tagFilters.add(new FilterPair(key, val));
                    } else if (type.equals("context")) {
                        contextFilters.add(new FilterPair(key, val));
                    } else {
                        throw new BadRequestException("Invalid filter type. Expected 'tag' or 'context'");
                    }
                } catch (Exception e) {
                    throw new BadRequestException("Invalid filter format. Expected comma-separated list of type:key:value triplets, e.g. tag:region:us-west-1,context:environment:prod. Key may only consist of numbers, letters and underscores", e);
                }
            }
            return new FilterSpec(tagFilters, contextFilters);
        }
    }

    public record TimestampParam(LocalDateTime timestamp) {
        public static TimestampParam fromString(String value) {
            try {
                return new TimestampParam(LocalDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME));
            } catch (DateTimeParseException e) {
                throw new BadRequestException("Invalid timestamp format in start or end timestamp. Expected ISO-8601 format with an optional timezone offset, e.g. 2021-01-01T00:00:00+01:00", e);
            }
        }
    }

    public enum SortOrder {
        ASC, DESC;

        public static SortOrder fromString(String value) {
            try {
                return SortOrder.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new BadRequestException("Invalid sort order. Expected 'asc' or 'desc'");
            }
        }
    }

    public enum AggregationType {
        AVG, SUM, COUNT, MIN, MAX;

        public static AggregationType fromString(String value) {
            return AggregationType.valueOf(value.toUpperCase());
        }
    }

    private static void ensureIdentifierIsSafe(String identifier) {
        if (!identifier.matches("^[a-zA-Z0-9_]+$")) {
            throw new BadRequestException("Invalid identifier. Expected only letters, numbers, and underscores");
        }
    }
}
