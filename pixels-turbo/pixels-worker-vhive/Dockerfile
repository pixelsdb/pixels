FROM openjdk:17
WORKDIR /root
COPY target/pixels-worker-vhive-jar-with-dependencies.jar pixels-worker-vhive.jar
EXPOSE 50051
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=100", "-jar", "pixels-worker-vhive.jar"]
