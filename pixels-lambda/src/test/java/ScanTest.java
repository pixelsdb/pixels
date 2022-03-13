import junit.framework.TestCase;
import junit.framework.TestResult;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;

import java.util.ArrayList;

public class ScanTest {
    LambdaClient lambdaClient = LambdaClient.builder()
            .region(Region.US_EAST_2)
            .build();
    Scan scan = new Scan();
    ArrayList<String> filesToScan = new ArrayList<String>() {
        {
            add("pixels-tpch-orders-v-0-order/20220312072707_0.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072714_1.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072720_2.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072727_3.pxlgit");
        }
    };

    ArrayList<String> cols = new ArrayList<String>() {
        {
            add("o_orderkey");
            add("o_custkey");
            add("o_orderstatus");
            add("o_orderdate");
        }
    };

    @Test
    public void testScan() {
        scan.scan(1, filesToScan, cols);
    }

    @Test
    public void testInvokeLambda() {
        scan.invokeLambda(lambdaClient, "Worker", filesToScan, cols);
    }
}