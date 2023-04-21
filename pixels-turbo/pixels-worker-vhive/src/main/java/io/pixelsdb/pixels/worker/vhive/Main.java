package io.pixelsdb.pixels.worker.vhive;

public class Main {
    private static final int PORT = 50051;
    public static void main(String[] args) {
        WorkerServer server = new WorkerServer(PORT);
        server.run();
    }
}
