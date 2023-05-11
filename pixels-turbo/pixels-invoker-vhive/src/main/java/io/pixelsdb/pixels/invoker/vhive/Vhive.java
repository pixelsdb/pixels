package io.pixelsdb.pixels.invoker.vhive;

public class Vhive {
    private static final Vhive instance = new Vhive();

    public static Vhive Instance() {
        return instance;
    }

    private final WorkerSyncClient client;
    private final WorkerAsyncClient asyncClient;

    private Vhive() {
        client = new WorkerSyncClient();
        asyncClient = new WorkerAsyncClient();
    }

    public WorkerSyncClient getSyncClient() {
        return client;
    }

    public WorkerAsyncClient getAsyncClient() {
        return asyncClient;
    }
}
