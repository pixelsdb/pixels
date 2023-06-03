package io.pixelsdb.pixels.worker.vhive;

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.worker.vhive.utils.Server;
import io.pixelsdb.pixels.worker.vhive.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class WorkerServer implements Server
{
    private static final Logger log = LogManager.getLogger(WorkerServer.class);
    private final io.grpc.Server rpcServer;
    private boolean running = false;

    public WorkerServer(int port)
    {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.rpcServer = ServerBuilder
                .forPort(port)
                .addService(new WorkerServiceImpl())
                .executor(Executors.newSingleThreadExecutor(Executors.defaultThreadFactory()))
                .build();
    }

    @Override
    public boolean isRunning()
    {
        return this.running;
    }

    @Override
    public void shutdown()
    {
        this.running = false;
        try
        {
            log.info("rpc server is trying to shutdown");
            FileAppender appender = LoggerContext.getContext().getConfiguration().getAppender("log");
            String logFilename = appender.getFileName();
            Utils.append(logFilename, logFilename);
            this.rpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            log.info("rpc server close successfully");
        } catch (InterruptedException e)
        {
            log.error("Interrupted when shutdown rpc server.", e);
        } catch (IOException e)
        {
            log.error("Append log failed when shutdown rpc server.", e);
        }
    }

    @Override
    public void run()
    {
        try
        {
            this.rpcServer.start();
            this.running = true;
            log.info("rpc server run successfully");
            this.rpcServer.awaitTermination();
        } catch (IOException e)
        {
            log.error("I/O error when running.", e);
        } catch (InterruptedException e)
        {
            log.error("Interrupted when running.", e);
        }
    }
}
