package io.pixelsdb.pixels.worker.vhive;

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.worker.vhive.utils.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerServer implements Server {
    private static Logger log = LogManager.getLogger(WorkerServer.class);
    private final io.grpc.Server rpcServer;
    private boolean running = false;

    public WorkerServer(int port) {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.rpcServer = ServerBuilder.forPort(port).addService(new WorkerServiceImpl()).build();
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public void shutdown() {
        this.running = false;
        try {
            this.rpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted when shutdown rpc server.", e);
        }
    }

    @Override
    public void run() {
        try {
            this.rpcServer.start();
            this.running = true;
            this.rpcServer.awaitTermination();
        } catch (IOException e) {
            log.error("I/O error when running.", e);
        } catch (InterruptedException e) {
            log.error("Interrupted when running.", e);
        } finally {
            this.shutdown();
        }
    }
}
