import org.junit.Assert;
import org.junit.Test;

public class WorkerTest {
    Worker worker = new Worker();
    //'{ "bucketName":"pixels-tpch-customer-v-0-order", "fileName": "20220213140252_0.pxl" }'
    String[] cols = {"n_nationkey", "n_name", "n_regionkey", "n_comment"};

    @Test
    public void testScanFileCanGrabColumnWithCorrectType() {
        String result =  worker.scanFile("20220213140252_0.pxl", 1024, cols);
        String expected = "success";
        Assert.assertEquals(result, expected);
    }
}