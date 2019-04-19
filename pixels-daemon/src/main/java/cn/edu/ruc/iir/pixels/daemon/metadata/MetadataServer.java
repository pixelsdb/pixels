package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.daemon.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created at: 19-4-17
 * Author: hank
 */
public class MetadataServer implements Server
{
    private static Logger log = LogManager.getLogger(MetadataServer.class);

    private boolean running = false;
    private final io.grpc.Server rpcServer;

    public MetadataServer(int port)
    {
        assert (port > 0 && port <= 65535);
        this.rpcServer = ServerBuilder.forPort(port)
                .addService(new MetadataServiceImpl())
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
            this.rpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e)
        {
            log.error("Interrupted when shutdown rpc server.", e);
        }
        DBUtil.Instance().close();
    }

    @Override
    public void run()
    {
        try
        {
            this.rpcServer.start();
            this.running = true;
            this.rpcServer.awaitTermination();
        } catch (IOException e)
        {
            log.error("I/O error when running.", e);
        } catch (InterruptedException e)
        {
            log.error("Interrupted when running.", e);
        } finally
        {
            this.shutdown();
        }
    }
}
