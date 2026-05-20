# ===== Build stage =====
# Use the official Maven image which has Maven + Eclipse Temurin JDK 21 pre-installed.
FROM maven:3.9.9-eclipse-temurin-21-alpine AS build
WORKDIR /workspace

# Cache dependencies — copy pom first, resolve deps, then copy sources.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src src
RUN mvn -B -DskipTests package && \
    mkdir -p target/extracted && \
    java -Djarmode=layertools -jar target/market-data-service.jar extract --destination target/extracted

# ===== Runtime stage =====
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Non-root user (OpenShift requirement: must run as arbitrary uid in 1000-65535)
RUN addgroup -S app && adduser -S -G app -u 1001 app
USER 1001

COPY --from=build /workspace/target/extracted/dependencies/         ./
COPY --from=build /workspace/target/extracted/spring-boot-loader/   ./
COPY --from=build /workspace/target/extracted/snapshot-dependencies/ ./
COPY --from=build /workspace/target/extracted/application/          ./

EXPOSE 8080

ENV JAVA_OPTS="-XX:+UseG1GC -XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]
