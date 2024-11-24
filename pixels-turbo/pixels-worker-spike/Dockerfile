# 使用 OpenJDK 作为基础镜像
FROM --platform=$TARGETPLATFORM openjdk:11-jre-slim

ARG HANDLER_JAR_FILE
ARG IMPL_JAR_FILE

# 创建工作目录
WORKDIR /app
RUN mkdir -p /app/etc
ENV PIXELS_HOME=/app/
COPY pixels.properties /app/etc/pixels.properties
# 复制 JAR 文件到镜像中
COPY ${HANDLER_JAR_FILE} /app/spike-java-handler.jar
COPY ${IMPL_JAR_FILE} /app/spike-java-handler-impl.jar

# 定义容器启动时的命令
#CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005", "-jar", "/app/spike-java-handler.jar", "/app/spike-java-handler-impl.jar"]
CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005", "-Xms8G", "-Xmx30G", "-jar", "/app/spike-java-handler.jar", "/app/spike-java-handler-impl.jar"]