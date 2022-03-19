package io.pixelsdb.pixels.lambda;

import java.util.ArrayList;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

public class QueryExecutor {
    public static void main(String[] args) {
        /* use lambda function to do the scan */
        ArrayList<String> filesToScan = new ArrayList<String>();


        ArrayList<String> cols = new ArrayList<String>() {
            {
                add("o_orderkey");
                add("o_custkey");
                add("o_orderstatus");
                add("o_orderdate");
            }
        };
        String[] colsArr = cols.toArray(new String[0]);

        //for testing on local
//        filesToScan.add("pixels-tpch-orders-v-0-order/20220312072707_0.pxl");
//        filesToScan.add("pixels-tpch-orders-v-0-order/20220312072714_1.pxl");
//        filesToScan.add("pixels-tpch-orders-v-0-order/20220312072720_2.pxl");
//        filesToScan.add("pixels-tpch-orders-v-0-order/20220312072727_3.pxl");

        // get the list of files in bucket to scan
        String bucketName = "pixels-tpch-orders-v-0-order";
        Region region = Region.US_EAST_2;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
//        filesToScan = listBucketObjects(s3, bucketName);
        for (String obj:listBucketObjects(s3, bucketName).subList(0,128)) {
            filesToScan.add(obj);
        }
        String[] filesToScanArr = filesToScan.toArray(new String[0]);
        s3.close();
        System.out.println(Arrays.toString(filesToScanArr));


        Scan scanLambda = new Scan();
        Scan scanNoLambda = new Scan();
        int numFilePerWorker=1;
        if (args.length==1) numFilePerWorker = Integer.parseInt(args[0]);

        /* use Pixels reader to scan tables directly */
        System.out.println("begin scan tables directly without using lambda");
        long startTimeNoLambda = System.nanoTime();

        scanNoLambda.readersReadFiles(filesToScanArr, colsArr);

        long endTimeNoLambda = System.nanoTime();
        double scanTimeNoLambda = 1.0*(endTimeNoLambda - startTimeNoLambda)/Math.pow(10, 9);

        /* use lambda function to do the scan */
        System.out.println("begin using lambda to scan files");
        long startTimeLambda = System.nanoTime();
        scanLambda.scan(numFilePerWorker, filesToScan, cols);
        long endTimeLambda = System.nanoTime();
        double scanTimeLambda = 1.0*(endTimeLambda - startTimeLambda)/Math.pow(10, 9);

        System.out.println("scanTimeLambda:" + scanTimeLambda);
        //System.out.println("scanTimeNoLambda:" + scanTimeNoLambda);
    }

    public static ArrayList<String> listBucketObjects(S3Client s3, String bucketName) {

        ArrayList<String> filesToScan = new ArrayList<String>();
        try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                S3Object myValue = (S3Object) iterVals.next();
                filesToScan.add(bucketName + "/" + myValue.key());
            }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return filesToScan;
    }
}
