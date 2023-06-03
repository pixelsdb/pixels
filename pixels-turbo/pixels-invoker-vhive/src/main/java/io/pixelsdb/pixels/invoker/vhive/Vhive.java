package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

public class Vhive
{
    private static final ConfigFactory config = ConfigFactory.Instance();
    private static final Vhive instance = new Vhive();
    private final WorkerAsyncClient asyncClient;

    private Vhive()
    {
        String hostname = config.getProperty("vhive.hostname");
        int port = Integer.parseInt(config.getProperty("vhive.port"));
        asyncClient = new WorkerAsyncClient(hostname, port);
    }

    public static Vhive Instance()
    {
        return instance;
    }

    public WorkerAsyncClient getAsyncClient()
    {
        return asyncClient;
    }
}
