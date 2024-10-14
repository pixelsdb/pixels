# Pixels-Worker-vHive

This module is for creating the Docker image for vHive cloud functions.

After [building Pixels](https://github.com/pixelsdb/pixels#build-pixels), we have the executable JAR file `target/pixels-worker-vhive-jar-with-dependencies.jar`.
Also modify the following items in `pixels.properties`:
```properties
# which cloud function service to use, can be lambda (AWS Lambda) or vhive (vHive)
executor.function.service=vhive
executor.ordered.layout.enabled=false
executor.compact.layout.enabled=true
# parameter for vhive client
vhive.hostname=[Your serverless endpoint, acquired by `kn service list` after deployment]
vhive.port=80
## or localhost:50051 if you are locally running your worker Docker image without deployment
```

## Docker Build

After `docker` installation, use the `Dockerfile` under the root directory of `pixels-worker-vhive` to build the docker image:

```bash
# run the following command under the root directory of pixels-worker-vhive module
docker build -f ./Dockerfile -t <your-docker-username>/<your-docker-image-name>:<your-image-tag> .
```

## Docker Run

First of all, test the Docker image locally.
Please pass the required environment variables by the `--env` option to correctly start the gRPC server in docker.

```bash
docker run -p 50051:50051 --rm --network=bridge \
--env PROFILING_ENABLED=true \
--env PROFILING_EVENT=wall \
--env FTP_HOST=<your-ftp-host> \
--env FTP_PORT=<your-ftp-port> \
--env FTP_USERNAME=<your-ftp-username> \
--env FTP_PASSWORD=<your-ftp-password> \
<your-docker-username>/<your-docker-image-name>:<your-image-tag>
```
... where you can also use `false` for `PROFILING_ENABLED`, and `alloc` for `PROFILING_EVENT`.

As for vHive function deployment, these environment variables are specified in the `configs/knative_workload/worker.yaml`([vHive project directory](https://github.com/pixelsdb/vhive)). Modify them accordingly before deployment.

## Docker Push

If the function image works well inside the local docker container, you can push it to your own
registry for the vHive worker to pull.
Note that Docker uses the docker hub by default, unless otherwise specified.

```bash
docker push <your-docker-username>/<your-docker-image-name>:<your-image-tag>
```

## FTP Service for Debugging and Profiling

As you can see, we have FTP related environment variables in the configuration.
This is because the function image will generate the logging file and profiling file during the execution, but the Firecracker microVM
where the image resides is short-lived.
Therefore, we need a persistent location to store such files for analysis.

Now our module use FTP service as a solution.
You can follow [this tutorial](https://ubuntu.com/server/docs/service-ftp)
and [this one](https://www.digitalocean.com/community/tutorials/how-to-set-up-vsftpd-for-a-user-s-directory-on-ubuntu-20-04)
to set up an FTP server easily.
