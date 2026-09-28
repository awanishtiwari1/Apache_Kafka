# Build Stage
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app

# Copy project definition and wrapper files
COPY pom.xml .
COPY .mvn .mvn
COPY mvnw .
COPY mvnw.cmd .

# Pre-fetch dependencies
RUN ./mvnw dependency:go-offline "-Djava.version=21"

# Copy application source code
COPY src ./src

# Build production JAR package
RUN ./mvnw clean package "-DskipTests" "-Djava.version=21"

# Runtime Stage with Java 21
FROM eclipse-temurin:21-jre
WORKDIR /app

# Copy built JAR artifact
COPY --from=build /app/target/*.jar app.jar

# Expose Spring Boot HTTP port
EXPOSE 8080

# Environment defaults
ENV SPRING_PROFILES_ACTIVE=default
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Entrypoint command
ENTRYPOINT ["java", "-jar", "app.jar"]
