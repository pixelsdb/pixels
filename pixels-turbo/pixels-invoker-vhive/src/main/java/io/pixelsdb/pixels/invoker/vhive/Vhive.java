package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

public class Vhive {
    private static final ConfigFactory config = ConfigFactory.Instance();
    private static final Vhive instance = new Vhive();
    private final WorkerSyncClient client;
    private final WorkerAsyncClient asyncClient;

    private Vhive() {
        String hostname = config.getProperty("vhive.hostname");
        int port = Integer.parseInt(config.getProperty("vhive.port"));
        client = new WorkerSyncClient(hostname, port);
        asyncClient = new WorkerAsyncClient(hostname, port);
    }

    public static Vhive Instance() {
        return instance;
    }

    public WorkerSyncClient getSyncClient() {
        return client;
    }

    public WorkerAsyncClient getAsyncClient() {
        return asyncClient;
    }
}
