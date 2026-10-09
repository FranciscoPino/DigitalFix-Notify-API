package cl.duoc.digitalfix.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

import cl.duoc.digitalfix.config.RabbitMQConfig;
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
            // TODO: enviar email/push al cliente (asignada, técnico en camino, cerrada)
        });
    }

    @RabbitListener(queues = RabbitMQConfig.Q_DISPATCH)
    public void onDispatch(Message message, Channel channel) throws IOException {
        processor.process(RabbitMQConfig.Q_DISPATCH, message, channel, env -> {
            log.info("[DISPATCH] type={} eventId={} payload={}", env.type(), env.eventId(), env.payload());
            // TODO: generar ticket de despacho / orden de terreno para el técnico
        });
    }

    @RabbitListener(queues = RabbitMQConfig.Q_REPORT)
    public void onReport(Message message, Channel channel) throws IOException {
        processor.process(RabbitMQConfig.Q_REPORT, message, channel, env -> {
            log.info("[REPORT] type={} eventId={} payload={}", env.type(), env.eventId(), env.payload());
            // TODO: generar PDF (informe técnico o certificado de trabajo)
        });
    }
}
