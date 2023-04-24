FROM openjdk:8
COPY target/pixels-worker-vhive-jar-with-dependencies.jar pixels-worker-vhive.jar
EXPOSE 50051
ENTRYPOINT ["java", "-jar", "pixels-worker-vhive.jar"]
