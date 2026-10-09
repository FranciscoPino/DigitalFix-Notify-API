# ms-digitalfix-notify

Microservicio de notificaciones de DigitalFix. Actúa como **consumer de RabbitMQ** (Spring AMQP) y procesa tres tipos de comando: email/push al cliente, ticket de despacho al técnico y generación de PDF. No expone endpoints de dominio; solo actuator en el puerto `8086`.

> Estado: topología, consumo, ACK/NACK, idempotencia y métricas están implementados. La acción de negocio de cada handler (enviar email, crear ticket, generar PDF) sigue como `TODO` en [CommandConsumer](src/main/java/cl/duoc/digitalfix/consumer/CommandConsumer.java) y por ahora solo registra el mensaje en el log.

## Topología RabbitMQ

Declarada en [RabbitMQConfig](src/main/java/cl/duoc/digitalfix/config/RabbitMQConfig.java); Spring AMQP la crea al arrancar (durable).

### Exchanges

| Exchange | Tipo | Uso |
|---|---|---|
| `cmd.direct` | direct | Comandos con routing key exacta |
| `cmd.topic` | topic | Comandos con routing key por patrón |
| `cmd.dead.dlx` | direct | Dead-letter exchange; enruta a cada DLQ |

### Colas y bindings

| Cola principal | Propósito | DLQ | Binding direct | Binding topic |
|---|---|---|---|---|
| `q.cmd.email` | Email/push al cliente (asignada, técnico en camino, cerrada) | `q.cmd.email.dlq` | `email.send` | `email.*` |
| `q.cmd.dispatch` | Ticket de despacho / orden de terreno al técnico | `q.cmd.dispatch.dlq` | `dispatch.ticket` | `dispatch.#` |
| `q.cmd.report` | Generación de PDF (informe técnico o certificado de trabajo) | `q.cmd.report.dlq` | `report.gen` | `report.*` |

Cada cola principal define `x-dead-letter-exchange=cmd.dead.dlx` y `x-dead-letter-routing-key=<nombre de la DLQ>`. Cada DLQ está ligada a `cmd.dead.dlx` con su propio nombre como routing key.

```
Productor ──► cmd.direct / cmd.topic ──► q.cmd.* ──► CommandConsumer
                                            │ (NACK sin requeue)
                                            ▼
                                       cmd.dead.dlx ──► q.cmd.*.dlq
```

> Las colas se declaran con argumentos (DLX). Si ya existían en el broker con otros argumentos, RabbitMQ rechaza la redeclaración (`PRECONDITION_FAILED`): elimínalas antes de arrancar.

## Formato de mensaje (envelope)

Todos los mensajes deben ser JSON con este envelope ([MessageEnvelope](src/main/java/cl/duoc/digitalfix/messaging/MessageEnvelope.java)):

```json
{
  "type": "EMAIL_ORDER_ASSIGNED",
  "eventId": "7f1c2d9e-5a6b-4c0e-9d3a-1b2c3d4e5f60",
  "timestamp": "2026-10-09T13:00:00Z",
  "traceId": "trace-123",
  "correlationId": "order-456",
  "payload": { "orderId": 456, "customerEmail": "cliente@example.com" }
}
```

| Campo | Obligatorio | Descripción |
|---|---|---|
| `type` | Sí | Tipo de evento/comando |
| `eventId` | Sí | Identificador único; base de la idempotencia |
| `timestamp` | No | Instante de emisión (ISO-8601) |
| `traceId` | No | Trazabilidad entre servicios (se incluye en el MDC del log) |
| `correlationId` | No | Correlación de negocio, p. ej. la orden (MDC del log) |
| `payload` | No | Datos específicos del comando |

## Procesamiento y buenas prácticas

Lógica común en [MessageProcessor](src/main/java/cl/duoc/digitalfix/messaging/MessageProcessor.java).

- **ACK/NACK explícitos:** `spring.rabbitmq.listener.simple.acknowledge-mode=manual`.

| Situación | Acción | Métrica `result` |
|---|---|---|
| Procesado correctamente | `basicAck` | `ack` |
| `eventId` ya procesado | `basicAck` (se descarta) | `duplicate` |
| Falla del handler (primera vez) | `basicNack` con requeue | `retry` |
| Falla del handler tras reentrega | `basicNack` sin requeue → DLQ | `dlq` |
| JSON inválido o sin `type`/`eventId` | `basicNack` sin requeue → DLQ | `dlq` |

- **Idempotencia:** [IdempotencyStore](src/main/java/cl/duoc/digitalfix/messaging/IdempotencyStore.java) guarda `cola:eventId` durante 24 h. Es en memoria: con más de una réplica o tras reinicios debe sustituirse por Redis o una BD.
- **Prefetch:** `10` mensajes por consumer.
- **Reintentos:** un único reintento inmediato (reentrega). Para backoff con espera, usar una cola de retry con TTL.

## Métricas

Contador Micrometer `notify.messages` con tags `queue` y `result` (`ack`, `duplicate`, `retry`, `dlq`).

- Endpoints: `/actuator/metrics/notify.messages` y `/actuator/prometheus`.
- Tasa de DLQ por cola (PromQL):

```
sum by (queue) (rate(notify_messages_total{result="dlq"}[5m]))
  / sum by (queue) (rate(notify_messages_total[5m]))
```

También conviene alertar por mensajes acumulados en las `*.dlq` desde el management UI de RabbitMQ.

## Configuración

Variables en `.env` (o entorno), leídas por [application.yml](src/main/resources/application.yml):

| Variable | Por defecto | Descripción |
|---|---|---|
| `RABBITMQ_HOST` | `localhost` | Host del broker |
| `RABBITMQ_PORT` | `5672` | Puerto AMQP |
| `RABBITMQ_USERNAME` | `guest` | Usuario |
| `RABBITMQ_PASSWORD` | `guest` | Contraseña (no usar `guest` fuera de local) |
| `RABBITMQ_VHOST` | `/` | Virtual host |
| `SERVER_PORT` | `8086` | Puerto HTTP (actuator) |

Opciones del listener:

```yaml
spring:
  rabbitmq:
    listener:
      simple:
        acknowledge-mode: manual        # ACK/NACK lo decide MessageProcessor
        default-requeue-rejected: false # los rechazados van a la DLQ
        prefetch: 10
```

## Ejecutar y probar

Levantar RabbitMQ local (management UI en http://localhost:15672, `guest`/`guest`):

```powershell
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

```powershell
mvn clean verify
mvn spring-boot:run
```

Comprobaciones:

- `GET http://localhost:8086/actuator/health`
- En el management UI, publicar un mensaje en el exchange `cmd.direct` con routing key `email.send` y el envelope JSON de arriba; el log debe mostrar `[EMAIL] type=... eventId=...`.
- Publicar el mismo `eventId` dos veces: el segundo se descarta como `duplicate`.
- Publicar un JSON inválido: el mensaje termina en `q.cmd.email.dlq` y aumenta `notify.messages{result="dlq"}`.
