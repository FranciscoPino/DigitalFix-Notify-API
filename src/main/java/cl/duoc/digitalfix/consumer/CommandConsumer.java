package cl.duoc.digitalfix.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.digitalfix.config.RabbitMQConfig;
import cl.duoc.digitalfix.messaging.MessageEnvelope;
import cl.duoc.digitalfix.messaging.MessageProcessor;

/** Consumidor de comandos; el ACK/NACK, idempotencia y métricas viven en {@link MessageProcessor}. */
@Component
public class CommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(CommandConsumer.class);

    private final MessageProcessor processor;

    public CommandConsumer(MessageProcessor processor) {
        this.processor = processor;
    }

    @RabbitListener(queues = RabbitMQConfig.Q_EMAIL)
    public void onEmail(Message message, Channel channel) throws IOException {
        processor.process(RabbitMQConfig.Q_EMAIL, message, channel, env -> {
            log.info("[EMAIL] type={} eventId={} payload={}", env.type(), env.eventId(), env.payload());
            print("EMAIL", RabbitMQConfig.Q_EMAIL, env);
            // TODO: enviar email/push al cliente según el type (asignada, en camino,
            // inicio de trabajo, cancelada o cerrada)
        });
    }

    @RabbitListener(queues = RabbitMQConfig.Q_DISPATCH)
    public void onDispatch(Message message, Channel channel) throws IOException {
        processor.process(RabbitMQConfig.Q_DISPATCH, message, channel, env -> {
            log.info("[DISPATCH] type={} eventId={} payload={}", env.type(), env.eventId(), env.payload());
            print("DISPATCH", RabbitMQConfig.Q_DISPATCH, env);
            // TODO: generar ticket de despacho / orden de terreno para el técnico
        });
    }

    @RabbitListener(queues = RabbitMQConfig.Q_REPORT)
    public void onReport(Message message, Channel channel) throws IOException {
        processor.process(RabbitMQConfig.Q_REPORT, message, channel, env -> {
            log.info("[REPORT] type={} eventId={} payload={}", env.type(), env.eventId(), env.payload());
            print("REPORT", RabbitMQConfig.Q_REPORT, env);
            // TODO: generar PDF (informe técnico o certificado de trabajo)
        });
    }

    // Salida por consola solo con fines demostrativos (clase); en producción usar únicamente el logger.
    private void print(String tipo, String queue, MessageEnvelope env) {
        System.out.println("""

                >>>>>>>>>> CONSUMER [%s] <<<<<<<<<<
                Cola        : %s
                Type        : %s
                Event ID    : %s
                Timestamp   : %s
                Trace ID    : %s
                Correlation : %s
                Payload     : %s
                >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                """.formatted(tipo, queue, env.type(), env.eventId(), env.timestamp(),
                env.traceId(), env.correlationId(), env.payload()));
    }
}
