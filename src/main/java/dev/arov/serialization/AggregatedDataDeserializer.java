package dev.arov.serialization;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import io.spoud.kcc.data.AggregatedDataWindowed;

/**
 * For use in dev mode when we are producing messages as JSON (and not Avro)
 */
public class AggregatedDataDeserializer extends ObjectMapperDeserializer<AggregatedDataWindowed> {
    public AggregatedDataDeserializer() {
        super(AggregatedDataWindowed.class);
    }
}
