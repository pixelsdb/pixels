import io.pixelsdb.pixels.lambda.ExprTree;
import io.pixelsdb.pixels.lambda.Scan;
import org.junit.Test;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

import java.util.ArrayList;

public class ScanTest {
    LambdaAsyncClient lambdaClient = LambdaAsyncClient.builder()
            .httpClientBuilder(AwsCrtAsyncHttpClient.builder()
                    .maxConcurrency(4000))
            .region(Region.US_EAST_2)
            .build();
    Scan scan = new Scan();
    ArrayList<String> filesToScan = new ArrayList<String>() {
        {
            add("pixels-tpch-orders-v-0-order/20220312072707_0.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072714_1.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072720_2.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072727_3.pxl");
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
    String[] colsArr = cols.toArray(new String[0]);

    ExprTree exprTree = new ExprTree("o_orderkey", ExprTree.Operator.GT, "3000");

    /**
     * use four lambda workers to each scan one different file
     */
    @Test
    public void testScan4files4workers() {
        scan.scan(1, filesToScan, cols, exprTree);
    }

    @Test
    public void testScan4files2workers() {
        scan.scan(2, filesToScan, cols, exprTree);
    }

    @Test
    public void testInvokeLambda() {
        scan.invokeLambda(lambdaClient, "Worker", filesToScan, cols, exprTree);
    }

    @Test
    public void readResultFilesOnS3() {
        String[] s3files = {
//                "15081819-7771-4f22-97df-60f768578f6afile0",
//                "6ebb2063-7965-4ab1-b831-e684777f72e5file0",
//                "736294d8-43c8-41b0-9def-b40f6f3b701dfile0",
//                "eaaf8b5d-3fcf-44b5-a7e4-a07fb6ef4147file0"
//                "b0f4f2dd-735b-473b-ac79-ed3af2d75eabfile0"
                "05dc032a-0b98-4e00-a538-6c7c69744002file2"
        };
        for (int i=0; i<s3files.length; i++) {
            s3files[i] = "tiannan-test/" + s3files[i];
            scan.scanResultFile(s3files[i], 1024, colsArr);
        }
    }
}