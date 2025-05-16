# How to build

in x86_64:

```bash
docker buildx create --use
# docker buildx build --build-arg HANDLER_JAR_FILE={Your handler jar file path} --build-arg IMPL_JAR_FILE={Your handler implement jar file path} --platform linux/amd64,linux/arm64 -t {Your aws ECR}:{version} --push .
docker buildx build --build-arg JAR_FILE=spike-java-example/target/spike-java-example-1.0-SNAPSHOT.jar --platform linux/amd64 -t 013072238852.dkr.ecr.cn-north-1.amazonaws.com.cn/agentguo/spike-java-worker:2.7 --push .
docker buildx build --build-arg JAR_FILE=spike-java-example/target/spike-java-example-1.0-SNAPSHOT.jar --platform linux/amd64,linux/arm64 -t 013072238852.dkr.ecr.cn-north-1.amazonaws.com.cn/agentguo/spike-java-worker:1.0 --push .
```

in arm64:

```bash
docker buildx create --use
docker buildx build --build-arg HANDLER_JAR_FILE={Your handler jar file path} --build-arg IMPL_JAR_FILE={Your handler implement jar file path} --platform linux/amd64,linux/arm64 -t {Your aws ECR}:{version} --push .
docker buildx build --build-arg HANDLER_JAR_FILE=spike-java-handler/target/spike-java-handler-1.0-SNAPSHOT.jar --build-arg IMPL_JAR_FILE=spike-java-example/target/spike-java-example-1.0-SNAPSHOT.jar --platform linux/amd64,linux/arm64 -t 013072238852.dkr.ecr.cn-north-1.amazonaws.com.cn/agentguo/spike-java-worker:1.0 --push .
docker buildx build --build-arg HANDLER_JAR_FILE=target/spike-java-handler-1.0-SNAPSHOT.jar --build-arg IMPL_JAR_FILE=target/pixels-worker-spike-0.2.0-SNAPSHOT.jar --platform linux/amd64,linux/arm64 -t 013072238852.dkr.ecr.cn-north-1.amazonaws.com.cn/agentguo/spike-java-worker:2.7 --push .
```