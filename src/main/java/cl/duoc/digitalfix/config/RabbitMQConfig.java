package cl.duoc.digitalfix.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** TopologÃ­a RabbitMQ de DigitalFix: colas de comandos, sus DLQ y bindings direct/topic. */
@Configuration
public class RabbitMQConfig {

    public static final String EXCHANGE_DIRECT = "cmd.direct";
    public static final String EXCHANGE_TOPIC = "cmd.topic";
    public static final String EXCHANGE_DLX = "cmd.dead.dlx";

    public static final String Q_EMAIL = "q.cmd.email";
    public static final String Q_DISPATCH = "q.cmd.dispatch";
    public static final String Q_REPORT = "q.cmd.report";

    public static final String Q_EMAIL_DLQ = Q_EMAIL + ".dlq";
    public static final String Q_DISPATCH_DLQ = Q_DISPATCH + ".dlq";
    public static final String Q_REPORT_DLQ = Q_REPORT + ".dlq";

    @Bean
    DirectExchange directExchange() {
        return new DirectExchange(EXCHANGE_DIRECT, true, false);
    }

    @Bean
    TopicExchange topicExchange() {
        return new TopicExchange(EXCHANGE_TOPIC, true, false);
    }

    /** Exchange de dead-letter; enruta a cada DLQ usando el nombre de la DLQ como routing key. */
    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange(EXCHANGE_DLX, true, false);
    }

    // Colas principales (con DLX configurado)
    @Bean
    Queue emailQueue() {
        return mainQueue(Q_EMAIL, Q_EMAIL_DLQ);
    }

    @Bean
    Queue dispatchQueue() {
        return mainQueue(Q_DISPATCH, Q_DISPATCH_DLQ);
    }

    @Bean
    Queue reportQueue() {
        return mainQueue(Q_REPORT, Q_REPORT_DLQ);
    }

    // Dead letter queues
    @Bean
    Queue emailDlq() {
        return QueueBuilder.durable(Q_EMAIL_DLQ).build();
    }

    @Bean
    Queue dispatchDlq() {
        return QueueBuilder.durable(Q_DISPATCH_DLQ).build();
    }

    @Bean
    Queue reportDlq() {
        return QueueBuilder.durable(Q_REPORT_DLQ).build();
    }

    // Bindings DLQ
    @Bean
    Binding emailDlqBinding() {
        return BindingBuilder.bind(emailDlq()).to(deadLetterExchange()).with(Q_EMAIL_DLQ);
    }

    @Bean
    Binding dispatchDlqBinding() {
        return BindingBuilder.bind(dispatchDlq()).to(deadLetterExchange()).with(Q_DISPATCH_DLQ);
    }

    @Bean
    Binding reportDlqBinding() {
        return BindingBuilder.bind(reportDlq()).to(deadLetterExchange()).with(Q_REPORT_DLQ);
    }

    // Bindings direct
    @Bean
    Binding emailDirectBinding() {
        return BindingBuilder.bind(emailQueue()).to(directExchange()).with("email.send");
    }

    @Bean
    Binding dispatchDirectBinding() {
        return BindingBuilder.bind(dispatchQueue()).to(directExchange()).with("dispatch.ticket");
    }

    @Bean
    Binding reportDirectBinding() {
        return BindingBuilder.bind(reportQueue()).to(directExchange()).with("report.gen");
    }

    // Bindings topic
    @Bean
    Binding emailTopicBinding() {
        return BindingBuilder.bind(emailQueue()).to(topicExchange()).with("email.*");
    }

    @Bean
    Binding dispatchTopicBinding() {
        return BindingBuilder.bind(dispatchQueue()).to(topicExchange()).with("dispatch.#");
    }

    @Bean
    Binding reportTopicBinding() {
        return BindingBuilder.bind(reportQueue()).to(topicExchange()).with("report.*");
    }

    private Queue mainQueue(String name, String dlq) {
        return QueueBuilder.durable(name)
                .deadLetterExchange(EXCHANGE_DLX)
                .deadLetterRoutingKey(dlq)
                .build();
    }
}
