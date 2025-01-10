package dev.arov;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.spoud.kcc.data.AggregatedDataWindowed;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.duckdb.DuckDBConnection;
import org.eclipse.microprofile.reactive.messaging.*;

import jakarta.enterprise.context.ApplicationScoped;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Function;

@ApplicationScoped
public class MetricRepository {
    private DuckDBConnection connection;
    private static final String TABLE_NAME = "aggregated_data";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    }

    private void createTableIfNotExists(DuckDBConnection connection) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" +
                    "start_time INTEGER, " +
                    "end_time INTEGER, " +
                    "initial_metric_name VARCHAR, " +
                    "tags VARCHAR, " +
                    "context VARCHAR, " +
                    "value DOUBLE)");
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
                    appender.append(metric.getStartTime().toEpochMilli());
                    appender.append(metric.getEndTime().toEpochMilli());
                    appender.append(metric.getInitialMetricName());
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

    public double getMetricAverage(String metricName) {
        return getConnection()
                .map(conn -> {
                    try (var statement = conn.prepareStatement("SELECT AVG(value) FROM " + TABLE_NAME + " WHERE initial_metric_name = ?")) {
                        statement.setString(1, metricName);
                        var result = statement.executeQuery();
                        if (result.next()) {
                            return result.getDouble(1);
                        }
                    } catch (SQLException e) {
                        Log.error("Failed to get metric average", e);
                    }
                    return 0.0;
                })
                .orElse(0.0);
    }
}
