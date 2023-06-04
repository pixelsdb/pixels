# Pixels-Worker-Vhive

This module is created as the docker image for vHive function deployment.
Run `mvn package` to generate the executable JAR file in `target/pixels-worker-vhive-jar-with-dependencies.jar`.

## Docker Build

After `docker` installation, use the `Dockerfile` under the root directory to build the docker image:

```bash
# run the following command under the root directory of pixels-worker-vhive module
docker build -f ./Dockerfile -t <your-docker-username>/<your-docker-image-name>:<your-image-tag> .
```

## Docker Run

The docker image needs some environement variables to run successfully:

```properties
PROFILING_ENABLED=true | false
PROFILING_EVENT=wall | alloc
FTP_HOST=<your-ftp-host>
FTP_PORT=<your-ftp-port>
FTP_USERNAME=<your-ftp-username>
FTP_PASSWORD=<your-ftp-password>
```

If you try to run the docker image locally, then you must provide these environement variables by `--env` to correctly
start gRPC server.
The entire docker command to test the docker image is:

```bash
docker run -p 50051:50051 --rm --network=bridge \
--env PROFILING_ENABLED=true \
--env PROFILING_EVENT=wall \
--env FTP_HOST=<your-ftp-host> \
--env FTP_PORT=<your-ftp-port> \
--env FTP_USERNAME=<your-ftp-username> \
--env FTP_PASSWORD=<your-ftp-password> \
<your-docker-username>/<your-docker-image-name>:<your-iamge-tag>
```

As for vhive function deployment, these environment variables are specify in
the `configs/knative_workload/worker.yaml`([vHive project directory](https://github.com/pixelsdb/vHive)), modify them as
the real value.

## Docker Push

After you ensure the function image can correctly run inside the local docker container, you can push it to your own
registery for the vHive worker's `containerd` to pull.
Here we assume you choose the docker hub by default:

```bash
docker push <your-docker-username>/<your-docker-image-name>:<your-image-tag>
```

## FTP Service

As you can see, we have FTP related enviroment variables in the configuration.
This is because the image will generate the logging file and profiling file during the execution, and the function image
running inside the Firecracker microVM is temporary.
Therefore, we need a permenant location to store such files for analysis.

Now our module use FTP service as a solution.
You can follow the [this tutorial](https://ubuntu.com/server/docs/service-ftp)
and [this one](https://www.digitalocean.com/community/tutorials/how-to-set-up-vsftpd-for-a-user-s-directory-on-ubuntu-20-04)
to setup a FTP server easily.

