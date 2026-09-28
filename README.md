# Spring Boot Apache Kafka Demo Application

A modern, production-grade Spring Boot 4.x application with Apache Kafka integration, featuring asynchronous producers, event-driven consumers, daily date-based Notepad file logging (`logs/YYYY-MM-DD.txt`), interactive REST APIs, and a glassmorphism web dashboard.

---

## 🚀 Features

- **Spring Boot 4.x & Java 21**: Built using Spring 7 / Spring Boot 4 ecosystem.
- **Apache Kafka Integration**:
  - **Topics**: Auto-created `String-Message` and `JSON-Message` with multiple partitions.
  - **Producer**: `KafkaProducerService` sending plain text and structured JSON events (`OrderEvent`) with async callbacks.
  - **Consumer**: `KafkaConsumerService` listening via `@KafkaListener` with automatic offset handling.
- **Daily Date Notepad Logging**:
  - Automatically creates a `logs/` directory.
  - Appends all application execution logs into `logs/YYYY-MM-DD.txt` (e.g. `logs/2026-09-28.txt`).
  - Rollover occurs at midnight; logs are accessible via REST API or host Notepad.
- **Interactive Web UI**: Real-time glassmorphism web dashboard at `http://localhost:8080` with message publishing, live polling message feed, stats, topic filters, and live Notepad log viewer.
- **Docker & Docker Compose Ready**: Includes multi-stage `Dockerfile` and `docker-compose.yml` orchestrating ZooKeeper, Kafka Broker, Kafka UI, and Spring Boot App.

---

## 🧠 Deep Dive: Apache Kafka Architecture, Implementation & Code Mechanics

Below is the complete architectural breakdown of every Kafka component, detailing **what it is**, **which dependencies are used**, **which classes & annotations implement it**, and **how it is created and utilized** in this application.

---

### 1. Kafka Broker & Cluster
- **Definition**: A **Kafka Broker** is an active server instance that receives, stores, index-tracks, and serves streaming records. A **Kafka Cluster** is a group of one or more brokers working together to deliver high availability, scalability, and data replication.
- **Dependencies Used**: `confluentinc/cp-kafka:7.6.0` (Docker Image in `docker-compose.yml`), `spring-boot-starter-kafka` (Maven dependency in `pom.xml`).
- **Configuration & Execution**:
  - **Docker Service (`docker-compose.yml`)**:
    ```yaml
    kafka:
      image: confluentinc/cp-kafka:7.6.0
      container_name: kafka-demo-broker
      ports: ["9092:9092"]
      environment:
        KAFKA_BROKER_ID: 1
        KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
        KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
    ```
  - **Spring Boot Property (`application.yml`)**:
    `spring.kafka.bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}`
  - **How Utilized**: When Spring Boot starts, the underlying `KafkaAdmin` and `KafkaTemplate` connect to the broker specified at `localhost:9092` (or `kafka:29092` inside Docker) to manage topics and transmit records.

---

#### 🌐 Cluster Comparison: Kafka Cluster vs. Kubernetes (K8s) Cluster
A **Cluster** refers to any group of interconnected machines (nodes) operating as a single unified system.

| Aspect | 🔴 Kafka Cluster | ☸️ Kubernetes (K8s) Cluster |
| :--- | :--- | :--- |
| **Primary Purpose** | Distributed event streaming & log storage platform | General container orchestration & app management platform |
| **Core Managed Unit** | Topics, Partitions, Offsets, & Message Brokers | Pods, Services, Deployments, & Worker Nodes |
| **Storage Role** | Stores persistent immutable event logs on disk | Allocates CPU/RAM resources & manages container lifecycles |
| **Key Goal** | High-throughput data ingestion, streaming & pub/sub | Auto-scaling, self-healing, rolling updates & service discovery |
| **Co-existence** | Can run natively or inside K8s | Can host and manage Kafka clusters (e.g., using Strimzi Operator) |

---

### 2. ZooKeeper
- **Definition**: Apache ZooKeeper is a centralized configuration and synchronization service. It maintains cluster topology, manages broker registration, and conducts leader elections for topic partitions.
- **Dependencies Used**: `confluentinc/cp-zookeeper:7.6.0` (Docker Image in `docker-compose.yml`).
- **Configuration & Execution**:
  - **Docker Service (`docker-compose.yml`)**:
    ```yaml
    zookeeper:
      image: confluentinc/cp-zookeeper:7.6.0
      container_name: kafka-demo-zookeeper
      ports: ["2181:2181"]
      environment:
        ZOOKEEPER_CLIENT_PORT: 2181
        ZOOKEEPER_TICK_TIME: 2000
    ```
  - **How Utilized**: ZooKeeper tracks the health of `kafka-demo-broker` on port `2181`. When `kafka-demo-broker` boots up, it registers itself with ZooKeeper.

---

### 3. Topics & Partitions
- **Definition**:
  - **Topic**: A logical stream channel or category name where messages are published.
  - **Partition**: A physical partition of a topic stream log distributed across brokers. Partitions enable horizontal scaling and concurrent processing by multiple consumers.
- **Dependencies Used**: `org.springframework.kafka.config.TopicBuilder`, `org.apache.kafka.clients.admin.NewTopic`.
- **Classes & Annotations Responsible**:
  - **Class**: [`KafkaTopicConfig.java`](file:///c:/Users/awani/Desktop/Apache_Kafka/src/main/java/com/example/kafka/config/KafkaTopicConfig.java)
  - **Annotations**: `@Configuration`, `@Bean`
- **How Created in Code**:
  ```java
  @Configuration
  public class KafkaTopicConfig {
      public static final String STRING_TOPIC = "String-Message";
      public static final String JSON_TOPIC = "JSON-Message";

      @Bean
      public NewTopic stringTopic() {
          return TopicBuilder.name(STRING_TOPIC).partitions(3).replicas(1).build();
      }

      @Bean
      public NewTopic jsonTopic() {
          return TopicBuilder.name(JSON_TOPIC).partitions(3).replicas(1).build();
      }
  }
  ```
- **How Utilized**: Upon application context initialization, Spring Boot's automatically configured `KafkaAdmin` bean reads these `NewTopic` definitions and executes admin commands to create `String-Message` and `JSON-Message` with 3 partitions each on the broker if they do not already exist.

---

### 4. Producer
- **Definition**: The application service that builds and publishes message records to Kafka topics.
- **Dependencies Used**: `org.springframework.kafka.core.KafkaTemplate`, `org.springframework.kafka.support.SendResult`.
- **Classes & Annotations Responsible**:
  - **Class**: [`KafkaProducerService.java`](file:///c:/Users/awani/Desktop/Apache_Kafka/src/main/java/com/example/kafka/producer/KafkaProducerService.java)
  - **Annotations**: `@Service`, `@RestController` (invoked by [`KafkaController.java`](file:///c:/Users/awani/Desktop/Apache_Kafka/src/main/java/com/example/kafka/controller/KafkaController.java))
- **Serialization Configuration (`application.yml`)**:
  ```yaml
  spring:
    kafka:
      producer:
        key-serializer: org.apache.kafka.common.serialization.StringSerializer
        value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
  ```
- **How Utilized in Code**:
  - **Plain Text Messages**:
    ```java
    CompletableFuture<SendResult<String, Object>> future = 
        kafkaTemplate.send(KafkaTopicConfig.STRING_TOPIC, messageKey, message);
    ```
  - **Structured JSON Events**:
    ```java
    CompletableFuture<SendResult<String, Object>> future = 
        kafkaTemplate.send(KafkaTopicConfig.JSON_TOPIC, key, orderEvent);
    ```
  - `KafkaTemplate` converts the Java object/string into byte payloads using `JsonSerializer` / `StringSerializer` and transmits them to Kafka brokers asynchronously, triggering completion callbacks (`whenComplete`) to log partition numbers and assigned offsets.

---

### 5. Consumer & Consumer Groups
- **Definition**:
  - **Consumer**: An event listener service subscribing to topics to process arriving stream records.
  - **Consumer Group**: A designated logical group of consumers (`Messaging-App-group`). Kafka distributes partitions among consumers in the group so each partition is read by exactly one consumer instance in that group.
- **Dependencies Used**: `org.springframework.kafka.annotation.KafkaListener`, `org.apache.kafka.clients.consumer.ConsumerRecord`.
- **Classes & Annotations Responsible**:
  - **Class**: [`KafkaConsumerService.java`](file:///c:/Users/awani/Desktop/Apache_Kafka/src/main/java/com/example/kafka/consumer/KafkaConsumerService.java)
  - **Annotations**: `@Service`, `@KafkaListener`
- **Deserialization Configuration (`application.yml`)**:
  ```yaml
  spring:
    kafka:
      consumer:
        group-id: Messaging-App-group
        auto-offset-reset: earliest
        key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
        value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
        properties:
          spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
          spring.json.trusted.packages: "*"
  ```
- **How Utilized in Code**:
  ```java
  @Service
  public class KafkaConsumerService {

      @KafkaListener(topics = KafkaTopicConfig.STRING_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
      public void listenStringTopic(ConsumerRecord<String, Object> record) {
          log.info("Received from {}: payload='{}'", record.topic(), record.value());
          // Add to thread-safe in-memory feed
      }

      @KafkaListener(topics = KafkaTopicConfig.JSON_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
      public void listenJsonTopic(ConsumerRecord<String, Object> record) {
          log.info("Received OrderEvent from {}: payload='{}'", record.topic(), record.value());
          // Add to thread-safe in-memory feed
      }
  }
  ```
  Spring Boot starts background listener containers that poll Kafka partitions continuously. Upon receiving a record, `@KafkaListener` deserializes the payload and invokes the listener method.

---

### 6. Offsets
- **Definition**: An integer sequence index automatically assigned to every record written to a partition. It acts as an immutable position marker in the partition log.
- **Configuration & Mechanics**:
  - `auto-offset-reset: earliest`: Ensures that if a new consumer group connects without prior committed offsets, it starts reading from offset `0` (the beginning of the log).
  - **Reading Offsets in Code**:
    ```java
    long offset = record.offset();
    int partition = record.partition();
    ```
- **How Utilized**: Kafka tracks the offset position of `Messaging-App-group`. When `KafkaConsumerService` successfully processes a message, Spring Kafka commits the offset back to Kafka's internal `__consumer_offsets` topic so the app never re-reads processed messages on restart.

---

### 7. Message Key & Payload
- **Definition**:
  - **Message Key**: An optional identifier string (e.g. `user-123` or `ORD-9981`). Kafka applies a MurmurHash2 algorithm on the key to select the target partition, ensuring all messages sharing the same key land on the exact same partition in strict chronological order.
  - **Payload**: The actual data payload transmitted inside the message (String text or `OrderEvent` DTO).
- **Classes Responsible**: [`MessageDto.java`](file:///c:/Users/awani/Desktop/Apache_Kafka/src/main/java/com/example/kafka/dto/MessageDto.java) & [`OrderEvent.java`](file:///c:/Users/awani/Desktop/Apache_Kafka/src/main/java/com/example/kafka/dto/OrderEvent.java).

---

## 🐳 Deep Dive: Docker & Docker Compose

### Why do we need `Dockerfile`?
A **`Dockerfile`** is a script containing instructions to package an application into a portable container image.

#### Why it's essential:
1. **Environment Independence**: Guarantees the application runs identically on any system (Windows, Mac, Linux, Cloud) regardless of local JDK version.
2. **Multi-Stage Build Optimization**:
   - **Stage 1 (Build)**: Uses `maven:3.9.9-eclipse-temurin-21` to compile Java code and package the `.jar` executable.
   - **Stage 2 (Runtime)**: Uses a lightweight `eclipse-temurin:21-jre` image containing only what's required to execute `app.jar`, reducing image size and security attack surface.

### Why do we need `docker-compose.yml`?
While a `Dockerfile` builds a single container (`app`), real-world systems require multiple services (Kafka, ZooKeeper, UI) working in sync. **`docker-compose.yml`** orchestrates all these containers together with one simple command.

#### Services in our `docker-compose.yml`:
1. **`zookeeper`**: Runs Apache ZooKeeper for Kafka cluster metadata tracking.
2. **`kafka`**: Runs the Apache Kafka message broker.
3. **`kafka-ui`**: Provides a visual browser dashboard on port `8085` to inspect topics, messages, consumer groups, and cluster state.
4. **`app`**: Builds and runs our Spring Boot application container on port `8080`.

#### Network & Volume Highlights:
- **Service Networking**: Containers communicate seamlessly over an isolated Docker network using service aliases (e.g., `KAFKA_BOOTSTRAP_SERVERS: kafka:29092`).
- **Host Volume Mapping (`./logs:/app/logs`)**: Syncs the container's `/app/logs` folder with `./logs` on your Windows machine, so today's `YYYY-MM-DD.txt` Notepad file is immediately accessible locally.

---

## 📁 Project Structure

```
.
├── Dockerfile                  # Multi-stage Docker build file
├── docker-compose.yml          # Multi-container orchestration (Kafka, Zookeeper, UI, App)
├── logback-spring.xml          # Daily rolling file appender configuration (logs/YYYY-MM-DD.txt)
├── pom.xml                     # Maven project configuration
├── README.md                   # Complete application documentation
└── src
    └── main
        ├── java
        │   └── com
        │       └── example
        │           └── kafka
        │               ├── KafkaDemoApplication.java  # Spring Boot Main Entrypoint
        │               ├── config
        │               │   └── KafkaTopicConfig.java  # Topic & Admin configuration (@Configuration, @Bean)
        │               ├── consumer
        │               │   └── KafkaConsumerService.java # Event Listener service (@KafkaListener)
        │               ├── controller
        │               │   ├── KafkaController.java   # Kafka REST APIs (@RestController)
        │               │   └── LogController.java     # Daily Notepad Log File REST APIs (@RestController)
        │               ├── dto
        │               │   ├── MessageDto.java        # Message response object
        │               │   └── OrderEvent.java        # Order JSON payload model
        │               └── producer
        │                   └── KafkaProducerService.java # Async KafkaTemplate producer (@Service)
        └── resources
            ├── application.yml # Kafka and server properties
            ├── logback-spring.xml # Logging rules
            └── static
                └── index.html  # Live Dashboard UI & Notepad Log Viewer
```

---

## 🛠️ How to Run

### Option 1: Run Everything with Docker Compose (Recommended)

1. **Build and start all services** in detached mode:
   ```bash
   docker compose up --build -d
   ```

2. **Access Web Interfaces**:
   - **Spring Boot Live Dashboard & Log Viewer**: [http://localhost:8080](http://localhost:8080)
   - **Kafka UI Cluster Inspector**: [http://localhost:8085](http://localhost:8085)

3. **View Console Logs in Real Time**:
   - Stream logs for **all containers**:
     ```bash
     docker compose logs -f
     ```
   - Stream logs for **Spring Boot App only**:
     ```bash
     docker compose logs -f app
     ```

4. **Stop and remove containers**:
   ```bash
   docker compose down
   ```

---

### Option 2: Run Kafka in Docker & App Locally

1. **Start Kafka & ZooKeeper containers**:
   ```bash
   docker compose up -d kafka zookeeper
   ```

2. **Run Spring Boot locally**:
   ```bash
   ./mvnw spring-boot:run
   ```
   *(or `mvnw.cmd spring-boot:run` on Windows CMD)*

3. **Open application**: [http://localhost:8080](http://localhost:8080)

---

## 📡 REST API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/kafka/publish?message=Hello&key=k1` | Publish text message to `String-Message` |
| `POST` | `/api/kafka/order` | Publish `OrderEvent` JSON payload to `JSON-Message` |
| `GET` | `/api/kafka/messages` | Retrieve list of consumed messages |
| `DELETE` | `/api/kafka/messages` | Clear stored consumed messages |
| `GET` | `/api/kafka/status` | Get producer/consumer stats & active topic counts |
| `GET` | `/api/logs/today` | Fetch today's `YYYY-MM-DD.txt` log file details & content |
| `GET` | `/api/logs/files` | List all stored daily notepad log files |
| `GET` | `/api/logs/download/{fileName}` | Download specific notepad `.txt` log file |
