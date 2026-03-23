# Dockerfile para Java 21
FROM eclipse-temurin:21-jdk-jammy
COPY target/beam-pipeline.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
