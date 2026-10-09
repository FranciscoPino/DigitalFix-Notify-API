package cl.duoc.digitalfix.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Procesa un mensaje con el flujo común: parseo del envelope, idempotencia,
 * ACK/NACK explícitos y métricas.
 *
 * Resultados (tag "result" de la métrica notify.messages): ack, duplicate, retry, dlq.
 * La tasa de DLQ se calcula como rate(result="dlq") / rate(total) por cola.
 */
@Component
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    @FunctionalInterface
    public interface Handler {
        void handle(MessageEnvelope envelope) throws Exception;
    }

    private final ObjectMapper mapper;
    private final IdempotencyStore idempotency;
    private final MeterRegistry metrics;

    public MessageProcessor(ObjectMapper mapper, IdempotencyStore idempotency, MeterRegistry metrics) {
        this.mapper = mapper.copy().findAndRegisterModules();
        this.idempotency = idempotency;
        this.metrics = metrics;
    }

    public void process(String queue, Message message, Channel channel, Handler handler) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        MessageEnvelope envelope;
        try {
            envelope = mapper.readValue(message.getBody(), MessageEnvelope.class);
        } catch (IOException e) {
            log.error("[{}] mensaje no parseable, enviando a DLQ: {}", queue, e.getMessage());
            reject(queue, channel, tag);
            return;
        }
        if (!envelope.isValid()) {
            log.error("[{}] envelope inválido (type/eventId obligatorios), enviando a DLQ", queue);
            reject(queue, channel, tag);
            return;
        }

        MDC.put("traceId", String.valueOf(envelope.traceId()));
        MDC.put("correlationId", String.valueOf(envelope.correlationId()));
        try {
            String key = queue + ":" + envelope.eventId();
            if (idempotency.alreadyProcessed(key)) {
                log.warn("[{}] eventId={} duplicado, se descarta", queue, envelope.eventId());
                count(queue, "duplicate");
                channel.basicAck(tag, false);
                return;
            }
            handler.handle(envelope);
            idempotency.markProcessed(key);
            channel.basicAck(tag, false);
            count(queue, "ack");
        } catch (Exception e) {
            if (message.getMessageProperties().isRedelivered()) {
                log.error("[{}] eventId={} falló de nuevo, enviando a DLQ", queue, envelope.eventId(), e);
                reject(queue, channel, tag);
            } else {
                log.warn("[{}] eventId={} falló, se reintenta una vez: {}", queue, envelope.eventId(), e.getMessage());
                channel.basicNack(tag, false, true);
                count(queue, "retry");
            }
        } finally {
            MDC.remove("traceId");
            MDC.remove("correlationId");
        }
    }

    private void reject(String queue, Channel channel, long tag) throws IOException {
        channel.basicNack(tag, false, false);
        count(queue, "dlq");
    }

    private void count(String queue, String result) {
        metrics.counter("notify.messages", "queue", queue, "result", result).increment();
    }
}
