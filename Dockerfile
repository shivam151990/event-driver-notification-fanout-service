FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY src ./src
RUN mvn -B -ntp package -DskipTests

FROM eclipse-temurin:21-jre-jammy
RUN groupadd --gid 10001 fanout && useradd --uid 10001 --gid fanout --no-create-home fanout
WORKDIR /app
COPY --from=build --chown=10001:10001 /workspace/target/*.jar /app/service.jar
COPY --chmod=0555 deploy/entrypoint.sh /app/entrypoint.sh
USER 10001:10001
EXPOSE 8080
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["/app/entrypoint.sh"]
