package io.pixelsdb.pixels.lambda;

import java.util.ArrayList;

public class QueryExecutor {
    public static void main(String[] args) {
        /* use lambda function to do the scan */
        ArrayList<String> filesToScan = new ArrayList<String>() {
            {
                add("pixels-tpch-orders-v-0-order/20220312072707_0.pxl");
                add("pixels-tpch-orders-v-0-order/20220312072714_1.pxl");
                add("pixels-tpch-orders-v-0-order/20220312072720_2.pxl");
                add("pixels-tpch-orders-v-0-order/20220312072727_3.pxl");
            }
        };
        String[] filesToScanArr = filesToScan.toArray(new String[0]);
        ArrayList<String> cols = new ArrayList<String>() {
            {
                add("o_orderkey");
                add("o_custkey");
                add("o_orderstatus");
                add("o_orderdate");
            }
        };
        String[] colsArr = cols.toArray(new String[0]);

        Scan scanLambda = new Scan();
        Scan scanNoLambda = new Scan();

        /* use lambda function to do the scan */
        System.out.println("begin using lambda to scan files");
        long startTimeLambda = System.nanoTime();
        scanLambda.scan(1, filesToScan, cols);
        long endTimeLambda = System.nanoTime();
        double scanTimeLambda = 1.0*(endTimeLambda - startTimeLambda)/Math.pow(10, 9);

        /* use Pixels reader to scan tables directly */
        System.out.println("begin scan tables directly without using lambda");
        long startTimeNoLambda = System.nanoTime();
        try {
            scanNoLambda.readersReadFiles(filesToScanArr, colsArr);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long endTimeNoLambda = System.nanoTime();
        double scanTimeNoLambda = 1.0*(endTimeNoLambda - startTimeNoLambda)/Math.pow(10, 9);

        System.out.println("scanTimeLambda:" + scanTimeLambda);
        System.out.println("scanTimeNoLambda:" + scanTimeNoLambda);
    }
}
