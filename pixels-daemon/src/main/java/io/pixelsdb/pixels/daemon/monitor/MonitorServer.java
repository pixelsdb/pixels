package io.pixelsdb.pixels.daemon.monitor;

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.common.server.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class MonitorServer implements Server {
    private static final Logger log = LogManager.getLogger(MonitorServer.class);
    private boolean running = false;
    private final io.grpc.Server rpcServer;
    private final int port;

    public MonitorServer(int port) {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.port = port;
        this.rpcServer = ServerBuilder.forPort(port)
                .addService(new MonitorServiceImpl()).build();

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
            log.error("interrupted when shutdown rpc server", e);
        }
    }

    @Override
    public void run() {
        try {
            this.rpcServer.start();
            System.out.println("Server started, listening on " + port);
            this.running = true;
            System.out.println("MonitorServer is running");
            this.rpcServer.awaitTermination();
        } catch (IOException e) {
            log.error("I/O error when running", e);
        } catch (InterruptedException e) {
            log.error("interrupted when running", e);
        } finally {
            this.shutdown();
        }
    }
}
