# Real-Time Event Streaming & Analytics with Spring Cloud Stream & Kafka Streams

A real-time event streaming and analytics application designed to process, aggregate, and visualize high-throughput page visit events. Built with Spring Boot, Spring Cloud Stream, Apache Kafka Streams, and real-time Server-Sent Events (SSE) chart visualization.

---

## Architecture Overview

```
                                      +-------------------------+
                                      |   pageEventSupplier     |
                                      |  (or /publish endpoint) |
                                      +------------+------------+
                                                   |
                                                   v
+------------------+                    +---------------------+
|                  | <================= |    Kafka Topic R2   |
| pageEventConsumer|                    +----------+----------+
+------------------+                               |
                                                   v
                                        +---------------------+
                                        |   KStreamFunction   |
                                        | (5s Window / Count) |
                                        +----------+----------+
                                                   |
                        +--------------------------+--------------------------+
                        |                                                     |
                        v                                                     v
              +--------------------+                               +--------------------+
              |   Kafka Topic R66  |                               | State Store (RocksDB)
              +--------------------+                               |   "count-store"    |
                                                                   +---------+----------+
                                                                             |
                                                                             v
                                                                   +--------------------+
                                                                   | Interactive Query  |
                                                                   | (/analytics SSE)   |
                                                                   +---------+----------+
                                                                             |
                                                                             v
                                                                   +--------------------+
                                                                   | Web UI (index.html)|
                                                                   |   SmoothieCharts   |
                                                                   +--------------------+
```

### Key Components

1. **Event Model (`PageEvents`)**:
   - Encapsulates event data: `name` (page name), `user` (user ID), `date` (timestamp), and `duree` (duration in ms).

2. **Event Production & Consumption (`PageEventHandler`)**:
   - `pageEventSupplier`: Automatically generates random page events (`Page 1`, `Page 2`) every second and routes them to topic `R2`.
   - `pageEventConsumer`: Consumes and logs incoming page events from `R2`.

3. **Stream Processing with Kafka Streams (`KStreamFunction`)**:
   - Ingests events from `R2`.
   - Filters events by duration (`duree >= 100ms`).
   - Groups events by page key.
   - Computes event counts in 5-second tumbling windows (`TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))`).
   - Materializes aggregations into a persistent state store named `count-store`.
   - Emits the aggregated window results to output topic `R66`.

4. **Interactive Query & Reactive SSE (`PageEventController`)**:
   - `/publish`: REST endpoint to manually inject page events into any Kafka topic using `StreamBridge`.
   - `/analytics`: Reactive `Flux` endpoint streaming windowed count metrics to clients every second using Server-Sent Events (SSE).

5. **Real-Time Visualization (`index.html`)**:
   - Connects to `/analytics` via `EventSource`.
   - Dynamically charts page visit counts over time using `SmoothieCharts`.

---

## Prerequisites

- **Java**: 17 or higher
- **Maven**: 3.8+ (or use the included `./mvnw`)
- **Docker & Docker Compose**: For launching Kafka and ZooKeeper

---

## Getting Started

### 1. Start Kafka & ZooKeeper

Launch the multi-container Kafka cluster defined in `compose.yml`:

```bash
docker compose up -d
```

Verify that the containers are healthy:

```bash
docker ps
```

### 2. Build and Run the Application

Start the Spring Boot application using Maven:

```bash
./mvnw clean spring-boot:run
```

The application runs on port `8089` by default.

### 3. Open Real-Time Dashboard

Navigate to your web browser:

```
http://localhost:8089/index.html
```

The real-time chart will dynamically display event counts per page (`Page 1`, `Page 2`, etc.) updated every second from the Kafka Streams state store.

---

## Endpoints

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `GET /analytics` | SSE | Streams windowed analytics (`text/event-stream`) in JSON format: `{"Page 1": 2, "Page 2": 5}`. |
| `GET /publish` | HTTP | Publishes a custom event to a topic. Params: `name` (page name), `topic` (target topic). |
| `GET /index.html` | HTTP | Web UI providing dynamic real-time visualization with SmoothieCharts. |

---

## Kafka CLI Utility Commands

To inspect Kafka topics directly from the broker container:

### Consume Raw Page Events (Topic `R2`)
```bash
docker exec -it broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic R2 \
  --from-beginning
```

### Produce Custom Events Manually (Topic `R2`)
```bash
docker exec -it broker kafka-console-producer \
  --bootstrap-server broker:29092 \
  --topic R2
```

### Consume Aggregated Analytics Stream (Topic `R66`)
```bash
docker exec -it broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic R66 \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

---

## Project Configuration Reference

Configuration settings are located in `src/main/resources/application.properties`:

- `server.port=8089`: HTTP web server port.
- `spring.cloud.function.definition=pageEventConsumer;pageEventSupplier;KStreamFunction`: Configured Spring Cloud Stream functions.
- `spring.cloud.stream.bindings.*`: Mapping of functional bindings to Kafka topics (`R2` for input/supplier, `R66` for analytics output).
- `default.deserialization.exception.handler`: Configured with `LogAndContinueExceptionHandler` for resilient stream execution.
