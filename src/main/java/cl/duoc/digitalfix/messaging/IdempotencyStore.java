package cl.duoc.digitalfix.messaging;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * Registro en memoria de eventId procesados, con TTL.
 * En despliegues con varias réplicas debe reemplazarse por un store compartido (Redis/BD).
 */
@Component
public class IdempotencyStore {

    private static final Duration TTL = Duration.ofHours(24);

    private final ConcurrentHashMap<String, Instant> processed = new ConcurrentHashMap<>();

    public boolean alreadyProcessed(String key) {
        Instant at = processed.get(key);
        if (at == null) {
            return false;
        }
        if (at.plus(TTL).isBefore(Instant.now())) {
            processed.remove(key, at);
            return false;
        }
        return true;
    }

    public void markProcessed(String key) {
        processed.put(key, Instant.now());
        if (processed.size() % 1000 == 0) {
            Instant limit = Instant.now().minus(TTL);
            processed.values().removeIf(t -> t.isBefore(limit));
        }
    }
}
