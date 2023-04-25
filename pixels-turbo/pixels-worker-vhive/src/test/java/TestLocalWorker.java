import com.google.common.base.Joiner;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.vhive.WorkerClient;
import io.pixelsdb.pixels.worker.vhive.WorkerServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestLocalWorker {
    private static final String HOST = "localhost";
    private static final int PORT = 50051;
    private final WorkerServer server = new WorkerServer(PORT);
    private final WorkerClient client = new WorkerClient(HOST, PORT);

    @Before
    public void startServer() throws InterruptedException {
        if (!server.isRunning()) {
            Thread thread = new Thread(server);
            thread.start();
            Thread.sleep(5000);
        }
    }

    @After
    public void endServer() throws InterruptedException {
        server.shutdown();
        Thread.sleep(5000);
        assertFalse(server.isRunning());
    }

    @Test
    public void testGrpcServer() {
        assertTrue(server.isRunning());
    }

    @Test
    public void testHello() {
        assertTrue(server.isRunning());

        String username = "zhaoshihan";
        assertEquals("Hello, " + username, client.hello(username));
    }

    @Test
    public void testAggregation() {
        assertTrue(server.isRunning());

        AggregationOutput output = client.aggregation(Utils.genAggregationInput());
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }

    @Test
    public void testBroadcastChainJoin() {
        assertTrue(server.isRunning());

        JoinOutput output = client.broadcastChainJoin(Utils.genBroadcastChainJoinInput());
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }

    @Test
    public void testBroadcastJoin() {
        assertTrue(server.isRunning());

        JoinOutput output = client.broadcastJoin(Utils.genBroadcastJoinInput());
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }

    @Test
    public void testPartitionChainJoin() {
        assertTrue(server.isRunning());

        JoinOutput output = client.partitionChainJoin(Utils.genPartitionedChainJoinInput());
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }

    @Test
    public void testPartitionJoin() {
        assertTrue(server.isRunning());

        JoinOutput output = client.partitionJoin(Utils.genPartitionedJoinInput());
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }

    @Test
    public void testPartition() {
        assertTrue(server.isRunning());

        PartitionOutput orderOutput = client.partition(Utils.genPartitionInput("order"));
        System.out.println(orderOutput.getPath());
        System.out.println(Joiner.on(",").join(orderOutput.getHashValues()));

        PartitionOutput lineitemOutput = client.partition(Utils.genPartitionInput("lineitem"));
        System.out.println(lineitemOutput.getPath());
        System.out.println(Joiner.on(",").join(lineitemOutput.getHashValues()));
    }

    @Test
    public void testScan() {
        assertTrue(server.isRunning());

        ScanOutput output = client.scan(Utils.genScanInput());
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }
}
