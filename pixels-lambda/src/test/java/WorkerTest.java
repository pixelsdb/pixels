import io.pixelsdb.pixels.lambda.Worker;
import org.junit.Assert;
import org.junit.Test;

public class WorkerTest {
    Worker worker = new Worker();
    //'{ "bucketName":"pixels-tpch-customer-v-0-order", "fileName": "20220213140252_0.pxl" }'
    String[] cols = {"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"};

    @Test
    public void testScanFileCanGrabColumnWithCorrectType() {
        String result =  worker.scanFile("pixels-tpch-orders-v-0-order/20220306043322_0.pxl", 1024, cols,"aaaaid123asdjjkhj88");
        String expected = "success";
        Assert.assertEquals(result, expected);
    }
}