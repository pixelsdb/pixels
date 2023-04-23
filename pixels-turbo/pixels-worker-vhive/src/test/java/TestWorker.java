import io.pixelsdb.pixels.worker.vhive.WorkerClient;
import io.pixelsdb.pixels.worker.vhive.WorkerServer;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestWorker {
    private static final String HOST = "localhost";
    private static final int PORT = 50051;

    @Test
    public void testGrpcServer() throws InterruptedException {
        WorkerServer server = new WorkerServer(PORT);
        Thread thread = new Thread(server);

        thread.start();
        Thread.sleep(5000);
        assertTrue(server.isRunning());

        server.shutdown();
        Thread.sleep(5000);
        assertFalse(server.isRunning());
    }

    @Test
    public void testGrpcClient() throws InterruptedException {
        WorkerServer server = new WorkerServer(PORT);
        Thread thread = new Thread(server);
        WorkerClient client = new WorkerClient(HOST, PORT);

        thread.start();
        Thread.sleep(5000);
        assertTrue(server.isRunning());

        server.shutdown();
        client.shutdown();
        Thread.sleep(5000);
        assertFalse(server.isRunning());
    }

    @Test
    public void testHello() throws InterruptedException {
        WorkerServer server = new WorkerServer(PORT);
        Thread thread = new Thread(server);
        WorkerClient client = new WorkerClient(HOST, PORT);

        thread.start();
        Thread.sleep(5000);
        String username = "zhaoshihan";
        assertEquals("Hello, " + username, client.hello(username));

        client.shutdown();
        server.shutdown();
        Thread.sleep(5000);
        assertFalse(server.isRunning());
    }


    @Test
    public void testScan() throws InterruptedException {
        WorkerServer server = new WorkerServer(PORT);
        Thread thread = new Thread(server);
        WorkerClient client = new WorkerClient(HOST, PORT);


    }
}
