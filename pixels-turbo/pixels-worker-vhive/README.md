# Pixels-Worker-Vhive

This module is to be used to create the docker image for vHive functions.
Run `mvn package` to generate the executable JAR file in `target/pixels-worker-vhive-jar-with-dependencies.jar`.

## Docker Build

After `docker` installation, use the `Dockerfile` under the root directory of `pixels-worker-hive` to build the docker image:

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

If you want to run the docker image locally, please provide these environement variables by `--env` to correctly
start the gRPC server in docker.
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

As for vHive function deployment, these environment variables are specify in
the `configs/knative_workload/worker.yaml`([vHive project directory](https://github.com/pixelsdb/vHive)), modify and set them to
the real values.

## Docker Push

If function image can correctly run inside the local docker container, you can push it to your own
registry for the vHive worker's `containerd` to pull.
Here we assume you choose the docker hub by default:

```bash
docker push <your-docker-username>/<your-docker-image-name>:<your-image-tag>
```

## FTP Service for Debuging and Profiling

As you can see, we have FTP related environment variables in the configuration.
This is because the image will generate the logging file and profiling file during the execution, and the function image
running inside the Firecracker microVM is temporary.
Therefore, we need a permanent location to store such files for analysis.

Now our module use FTP service as a solution.
You can follow [this tutorial](https://ubuntu.com/server/docs/service-ftp)
and [this one](https://www.digitalocean.com/community/tutorials/how-to-set-up-vsftpd-for-a-user-s-directory-on-ubuntu-20-04)
to set up an FTP server easily.

