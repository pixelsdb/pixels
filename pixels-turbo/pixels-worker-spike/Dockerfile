FROM eclipse-temurin:11-jre

# Build from the Pixels repository root with -f pixels-turbo/pixels-worker-spike/Dockerfile.
ARG PIXELS_CONFIG_FILE=pixels-common/src/main/resources/pixels.properties
ARG AWS_REGION

WORKDIR /app
ENV PIXELS_HOME=/app \
    PIXELS_CONFIG=/app/etc/pixels.properties \
    AWS_REGION=${AWS_REGION}

COPY ${PIXELS_CONFIG_FILE} /app/etc/pixels.properties
RUN sed -i 's/^enabled.storage.schemes=.*/enabled.storage.schemes=s3,s3qs/' /app/etc/pixels.properties
COPY pixels-turbo/pixels-worker-spike/target/spike-java-handler-0.1.1.jar /app/spike-java-handler.jar
COPY pixels-turbo/pixels-worker-spike/target/pixels-worker-spike-0.2.0-SNAPSHOT.jar /app/spike-java-handler-impl.jar

EXPOSE 50052

# The implementation jar carries the current Pixels protobuf and gRPC runtime.
# Put it first in the parent classpath used by Spike's parent-first InterfaceLoader.
ENTRYPOINT ["java", "-cp", "/app/spike-java-handler-impl.jar:/app/spike-java-handler.jar", "io.pixelsdb.spike.handler.InterfaceLoader", "/app/spike-java-handler-impl.jar"]
