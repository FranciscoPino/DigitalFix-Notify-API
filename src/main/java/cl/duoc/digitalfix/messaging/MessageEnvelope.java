package cl.duoc.digitalfix.messaging;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

/** Envelope común de todos los mensajes de comando. */
public record MessageEnvelope(
        String type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        JsonNode payload) {

    public boolean isValid() {
        return type != null && !type.isBlank() && eventId != null && !eventId.isBlank();
    }
}
