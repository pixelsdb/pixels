# Using OpenJDK as the base image
FROM --platform=$TARGETPLATFORM openjdk:11-jre-slim

ARG HANDLER_JAR_FILE
ARG IMPL_JAR_FILE

# Create a working directory
WORKDIR /app
RUN mkdir -p /app/etc
ENV PIXELS_HOME=/app/
COPY ../../pixels-common/src/main/resources/pixels.properties /app/etc/pixels.properties
# Copy JAR files to the image
COPY ${HANDLER_JAR_FILE} /app/spike-java-handler.jar
COPY ${IMPL_JAR_FILE} /app/spike-java-handler-impl.jar

# Define commands for container startup
#CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005", "-jar", "/app/spike-java-handler.jar", "/app/spike-java-handler-impl.jar"]
CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005", "-Xms8G", "-Xmx30G", "-jar", "/app/spike-java-handler.jar", "/app/spike-java-handler-impl.jar"]