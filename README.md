# ms-digitalfix-notify

Microservicio de notificaciones de DigitalFix. Actúa como **consumidor (consumer) de RabbitMQ** (Spring AMQP) de los comandos que publica `ms-digitalfix-workorders` (productor) y procesa tres tipos: email/push al cliente, ticket de despacho al técnico y generación de PDF. No expone endpoints de dominio; solo actuator en el puerto `8086`.

> Estado: topología, consumo, ACK/NACK, idempotencia y métricas están implementados. La acción de negocio de cada handler (enviar email, crear ticket, generar PDF) sigue como `TODO` en [CommandConsumer](src/main/java/cl/duoc/digitalfix/consumer/CommandConsumer.java) y por ahora solo registra el mensaje en el log y en consola.

## Rol en la arquitectura

```text
ms-digitalfix-workorders (productor) ──► RabbitMQ (cmd.direct) ──► ms-digitalfix-notify (consumer)
                                                                        │
                                       CommandConsumer ─► MessageProcessor ─► handler ─► ACK / NACK
```

| Clase | Responsabilidad |
|---|---|
| [RabbitMQConfig](src/main/java/cl/duoc/digitalfix/config/RabbitMQConfig.java) | Declara exchanges, colas, DLQ y bindings (el consumer es dueño de la topología) |
| [CommandConsumer](src/main/java/cl/duoc/digitalfix/consumer/CommandConsumer.java) | Un `@RabbitListener` por cola (`onEmail`, `onDispatch`, `onReport`) |
| [MessageProcessor](src/main/java/cl/duoc/digitalfix/messaging/MessageProcessor.java) | Parseo, validación, idempotencia, ACK/NACK, métricas y salida por consola |
| [IdempotencyStore](src/main/java/cl/duoc/digitalfix/messaging/IdempotencyStore.java) | Evita reprocesar un `eventId` ya visto |
| [MessageEnvelope](src/main/java/cl/duoc/digitalfix/messaging/MessageEnvelope.java) | Envelope común de los mensajes |

## Flujo de consumo de un mensaje

1. Llega un mensaje a `q.cmd.email`, `q.cmd.dispatch` o `q.cmd.report` y lo recibe el listener correspondiente.
2. `MessageProcessor` imprime el mensaje recibido y lo parsea como envelope.
3. Si el JSON es inválido o faltan `type`/`eventId` → `NACK` sin requeue → `cmd.dead.dlx` → `q.cmd.*.dlq`.
4. Si `cola:eventId` ya fue procesado → `ACK` y se descarta (`duplicate`).
5. Si no, ejecuta el handler del listener (log y salida por consola; aquí irá la lógica de negocio).
6. Éxito → `ACK`. Falla la primera vez → `NACK` con requeue (un reintento). Falla tras reentrega → `NACK` sin requeue → DLQ.

## Mensajes que consume (emitidos por workorders)

| Cola | Routing key | `type` | Se genera cuando la orden pasa a |
|---|---|---|---|
| `q.cmd.email` | `email.send` | `EMAIL_ORDER_ASSIGNED` | `ASSIGNED` |
| `q.cmd.email` | `email.send` | `EMAIL_TECHNICIAN_EN_ROUTE` | `EN_ROUTE` |
| `q.cmd.email` | `email.send` | `EMAIL_WORK_STARTED` | `IN_PROGRESS` |
| `q.cmd.email` | `email.send` | `EMAIL_ORDER_CLOSED` | `CLOSED` |
| `q.cmd.email` | `email.send` | `EMAIL_ORDER_CANCELLED` | `CANCELLED` |
| `q.cmd.dispatch` | `dispatch.ticket` | `DISPATCH_TICKET_REQUESTED` | `ASSIGNED` |
| `q.cmd.report` | `report.gen` | `REPORT_GENERATION_REQUESTED` | `CLOSED` |

El `payload` trae `orderId`, `customerId`, `serviceId`, `technicianId`, `status`, `title`, `description`, `address` y `observations`.

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
| `q.cmd.email` | Email/push al cliente (asignada, técnico en camino, trabajo iniciado, cancelada, cerrada) | `q.cmd.email.dlq` | `email.send` | `email.*` |
| `q.cmd.dispatch` | Ticket de despacho / orden de terreno al técnico | `q.cmd.dispatch.dlq` | `dispatch.ticket` | `dispatch.#` |
| `q.cmd.report` | Generación de PDF (informe técnico o certificado de trabajo) | `q.cmd.report.dlq` | `report.gen` | `report.*` |

Cada cola principal define `x-dead-letter-exchange=cmd.dead.dlx` y `x-dead-letter-routing-key=<nombre de la DLQ>`. Cada DLQ está ligada a `cmd.dead.dlx` con su propio nombre como routing key.

Las seis colas (principales y DLQ) son **quorum queues** (`x-queue-type=quorum`), por lo que RabbitMQ replica los mensajes entre los nodos del clúster. RabbitMQ replica automáticamente la metadata de exchanges, colas y bindings; para replicar el contenido de las colas se requiere un tipo de cola replicado como quorum. `queue_leader_locator=balanced` solo distribuye líderes de colas clásicas, no replica sus mensajes.

En un clúster de dos nodos, el quorum necesita ambos nodos disponibles para mantener mayoría. Si uno cae, las quorum queues dejan de aceptar operaciones hasta recuperar la mayoría. Para alta disponibilidad ante la caída de un nodo, configura tres nodos.

```
Productor ──► cmd.direct / cmd.topic ──► q.cmd.* ──► CommandConsumer
                                            │ (NACK sin requeue)
                                            ▼
                                       cmd.dead.dlx ──► q.cmd.*.dlq
```

> **Migración:** RabbitMQ no permite cambiar una cola classic existente a quorum redeclarando el mismo nombre. Migra con nombres nuevos y mueve los mensajes, o respalda/drena y elimina las colas existentes antes de recrearlas. Eliminar una cola descarta sus mensajes pendientes. Una diferencia en los argumentos DLX también provoca `PRECONDITION_FAILED`.

## Formato de mensaje (envelope)

Todos los mensajes deben ser JSON con este envelope ([MessageEnvelope](src/main/java/cl/duoc/digitalfix/messaging/MessageEnvelope.java)):

```json
{
  "type": "EMAIL_ORDER_ASSIGNED",
  "eventId": "7f1c2d9e-5a6b-4c0e-9d3a-1b2c3d4e5f60",
  "timestamp": "2026-10-09T13:00:00Z",
  "traceId": "trace-123",
  "correlationId": "order-456",
  "payload": { "orderId": "60663ba9-f13d-4f9a-a990-295f61d5cb08", "customerId": "cliente@example.com", "status": "ASSIGNED" }
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
| `RABBITMQ_ADDRESSES` | vacío | Direcciones del clúster separadas por coma, p. ej. `localhost:5672,localhost:5673`; si se configura, prevalece sobre host/port |
| `RABBITMQ_USERNAME` | `admin` | Usuario (el del `docker-compose.yml` del clúster local) |
| `RABBITMQ_PASSWORD` | `admin123` | Contraseña (valor de desarrollo; usar secretos fuera de local) |
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

Para el clúster local de dos nodos, configura `.env` tanto en notify como en workorders:

```env
RABBITMQ_ADDRESSES=localhost:5672,localhost:5673
RABBITMQ_USERNAME=admin
RABBITMQ_PASSWORD=admin123
RABBITMQ_VHOST=/
```

Usa los puertos y credenciales definidos por tu clúster. La lista permite al cliente conectarse a cualquiera de los nodos; la replicación de los mensajes la realiza el tipo quorum de las colas.

## Salida por consola (demostración)

Para cada mensaje, notify imprime en consola (solo con fines demostrativos; en producción usar únicamente el logger):

1. `MENSAJE RECIBIDO DE RABBITMQ`: cola, propiedades y cuerpo (`MessageProcessor`).
2. `CONSUMER [EMAIL|DISPATCH|REPORT]`: type, eventId, timestamp, traceId, correlationId y payload (listener de `CommandConsumer`; solo mensajes válidos y no duplicados).
3. `MENSAJE PROCESADO`: resultado `OK (ACK)`, `DUPLICADO`, `ERROR` o `RECHAZADO` (`MessageProcessor`).

Esa salida aparece en la terminal donde se ejecutó `mvn spring-boot:run`.

## Ejecutar y probar

Levantar RabbitMQ local. Con el clúster de `docker-compose.yml` (2 nodos) la UI está en http://localhost:15672 y http://localhost:15673 (`admin`/`admin123`). Para un solo nodo de pruebas:

```powershell
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

```powershell
mvn clean verify
mvn spring-boot:run
```

Notify debe iniciarse primero (o al menos una vez) para declarar colas y bindings; luego el productor puede publicar.

Comprobaciones:

- `GET http://localhost:8086/actuator/health`
- **Flujo real:** con workorders en ejecución, cambia el estado de una orden (front o `PUT http://localhost:8081/api/workorders/{id}/status`); en la consola de notify aparecen los tres bloques y `Resultado : OK (ACK)`.
- **Manual:** en el management UI, publicar en el exchange `cmd.direct` con routing key `email.send` y el envelope JSON de arriba.
- Publicar el mismo `eventId` dos veces: el segundo se descarta como `duplicate`.
- **Provocar la DLQ:** publicar un JSON inválido, o desde el front usar el bloque plegable "Demo RabbitMQ" (workorders `POST /api/workorders/demo/dlq?queue=email&mode=invalid-json`). El mensaje termina en `q.cmd.email.dlq`, la consola muestra `RECHAZADO` y aumenta `notify.messages{result="dlq"}`.
