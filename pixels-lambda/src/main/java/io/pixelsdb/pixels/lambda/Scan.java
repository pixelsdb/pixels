package io.pixelsdb.pixels.lambda;

import com.google.gson.Gson;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class Scan {
    String res = "";
    private static final Logger LOGGER = LogManager.getLogger(Scan.class);
    ArrayList<Thread> readS3ResultsThreads = new ArrayList<>();
    ExecutorService threadPool = Executors.newFixedThreadPool(8);

    /**
     * this function implements the scan operator. It invokes numWorker workers each scan one or more files
     * to finishing scan all files. Scanned results are written to S3. This functions then read scan results
     * on s3.
     * @param numFilePerWorker number of files each worker is responsible for reading
     * @param files list of file paths on s3 to scan
     */
    public void scan(int numFilePerWorker, ArrayList<String> files, ArrayList<String> cols) {
        long scanStart = System.nanoTime();
        String[] colsArr = cols.toArray(new String[0]);

        int numWorker = (int) Math.ceil((double) files.size() / numFilePerWorker);
        LambdaAsyncClient lambdaClient = LambdaAsyncClient.builder()
                .httpClientBuilder(AwsCrtAsyncHttpClient.builder()
                        .maxConcurrency(4000).connectionMaxIdleTime(Duration.ofSeconds(1000)))
                .region(Region.US_EAST_2)
                .build();

        // invoke numWorker lambda workers to each scan one or more files
        CompletableFuture<Void>[] lambdaFutures = new CompletableFuture[numWorker];
        for (int i=0; i<numWorker; i++) {
            List<String> filesForThisWorker;
            if (i<numWorker-1) {
                filesForThisWorker = files.subList(i*numFilePerWorker, (i+1)*numFilePerWorker);
            } else {
                // for last worker, handled files might be less than other workers
                filesForThisWorker = files.subList(i*numFilePerWorker, files.size());
            }

            lambdaFutures[i] = invokeLambda(lambdaClient, "Worker", filesForThisWorker, cols).thenAccept((invokeResponse)->readersReadFiles(responseToStrArr(invokeResponse), colsArr));

            // async invoke lambda functions each reads some files and then write results to s3
            // Each worker's response are result files s3 links.
            // when a response is received, create some readers to read the s3 links in the response
//            lambdaFutures[i] = CompletableFuture.supplyAsync(()->invokeLambda(lambdaClient, "Worker", filesForThisWorker, cols), threadPool);
                    //.thenAccept((invokeResponse)->readersReadFiles(responseToStrArr(invokeResponse), colsArr));
            LOGGER.info("start " + i + "th invocation");
        }

        try {
            CompletableFuture.allOf(lambdaFutures).get();
            LOGGER.info("all responses have been received and reader thread for each result file has been created.");
            for (int i=0; i<lambdaFutures.length; i++) {
                LOGGER.info("lambdaFutures["+i+"].isDone: "+lambdaFutures[i].isDone());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(300, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        double durationScan = 1.0 * (System.nanoTime() - scanStart) / Math.pow(10,9);
        LOGGER.info("durationScan: " +
                durationScan);

//        for (Thread t:readS3ResultsThreads) {
//            try {
//                t.join();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        double durationTillAllThreadsFinished = 1.0 * (System.nanoTime() - scanStart) / Math.pow(10,9);
//        LOGGER.info("all threads to read result files on s3 have finished");
//        LOGGER.info("durationTillAllThreadsFinished: " + durationTillAllThreadsFinished);
//        for (CompletableFuture<InvokeResponse> l:lambdaFutures) {
//            try {
//                InvokeResponse response = l.get();
//
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
//        }

        // keep checking if any invocation's response is available
        // if a response is available, create a new thread for each file in the response to read it
//        boolean[] workersResponseHandled = new boolean[numWorker];
//        for (int i=0; i<numWorker; i++) {
//            workersResponseHandled[i] = false;
//        }
//        boolean responsesAllHandled = false;
//
//        long startTimeWhileLoop = System.nanoTime();
//        while (!responsesAllHandled) {
//            //System.out.println(Arrays.toString(workersResponseHandled));
//            // check if there's worker's response ready to be handled
//            for (int i=0; i<numWorker; i++) {
//                if (lambdaFutures[i].isDone() && !workersResponseHandled[i]) {
//                    // handle the response for this worker
//                    try {
//                        InvokeResponse response = lambdaFutures[i].get();
//                        String resVal = response.payload().asUtf8String();
//                        System.out.println("resVal: "+ resVal);
//                        System.out.println("now use these s3 links to read the results read by the lambda worker");
//                        // for each s3file create a thread to read it
//                        resVal = resVal.replace("\"",""); // delete the "" in response as its json string
//                        String[] s3files = resVal.split(",");
//                        for (int j=0; j<s3files.length; j++) s3files[j] = "tiannan-test/" + s3files[j];
//                        System.out.println("s3files: " + Arrays.toString(s3files));
//                        readersReadFiles(s3files, colsArr);
//                        workersResponseHandled[i] = true;
//                    } catch (Exception e) {
//                        System.out.println("response" + i + "not ready");
//                    }
//                }
//            }
//            responsesAllHandled = true;
//            for (boolean handled:workersResponseHandled) {
//                if (!handled) {
//                    responsesAllHandled = false;
//                    break;
//                }
//            }
//        }
//        double durationWhileLoop = 1.0*(System.nanoTime() - startTimeWhileLoop)/Math.pow(10,6);
//        LOGGER.info("durationWhileLoopMs: "+ durationWhileLoop);
//        System.out.println("before exiting scan");
    }

    private String[] responseToStrArr(InvokeResponse invokeResponse) {
        String[] s3files = invokeResponse.payload().asUtf8String().replace("\"","").split(",");
        for (int i=0; i<s3files.length; i++) {
            s3files[i] = "tiannan-test/" + s3files[i];
        }
        return s3files;
    }

    /**
     * invoke a lambda worker instance which is responsible for reading a list of file
     * @param lambdaClient
     * @param functionName
     * @return
     */
    public CompletableFuture<InvokeResponse> invokeLambda(LambdaAsyncClient lambdaClient, String functionName, List<String> filesToScan, ArrayList<String> cols) {
        ArrayList<String> arrListFiles = new ArrayList<>();
        arrListFiles.addAll(filesToScan);
        CompletableFuture<InvokeResponse> res = null ;
        try {
            //create a json string like the one below as payload
            //            String json = "{ \"fileNames\":[\"pixels-tpch-orders-v-0-order/20220312072707_0.pxl\"],\n" + "\"cols\":[\"o_orderkey\", \"o_custkey\", \"o_orderstatus\", \"o_orderdate\"]}";
            LambdaEvent lambdaEvent = new LambdaEvent(arrListFiles, cols);
            String eventString = new Gson().toJson(lambdaEvent);
            System.out.println(eventString);
            SdkBytes payload = SdkBytes.fromUtf8String(eventString);

            //Setup an InvokeRequest
            InvokeRequest request = InvokeRequest.builder()
                    .functionName(functionName)
                    .payload(payload)
                    .invocationType("RequestResponse") //Event for async invocation, RequestResponse for sync invocation
                    .build();

            res = lambdaClient.invoke(request);

        } catch(LambdaException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return res;
    }

    public String scanFile(String fileName, int batchSize, String[] cols)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);
//        option.predicate();

        VectorizedRowBatch rowBatch;

        try (PixelsReader pixelsReader = getReader(fileName);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            LOGGER.info("Start reading file: " + fileName);
            TypeDescription allSchema = pixelsReader.getFileSchema();
            List<TypeDescription> allColTypes =  allSchema.getChildren();
            List<String> fieldNames = allSchema.getFieldNames();
//            LOGGER.info("fieldNames: " + fieldNames);
//            LOGGER.info("allColTypes" + allColTypes);
            TypeDescription queriedSchema = TypeDescription.createStruct();
            // for each queried col find its type
            ArrayList<TypeDescription> queriedColTypes = new ArrayList<>();
            for (int i=0; i<cols.length; i++) {
                // here assume fieldNames and colTypes are in same order
                queriedSchema.addField(cols[i], allColTypes.get(fieldNames.indexOf(cols[i])));
            }

            int batch = 0;
            while (true)
            {
                rowBatch = recordReader.readBatch(batchSize);
                //LOGGER.info(rowBatch.toString());
                // TODO log out the content of the row batches
                if (rowBatch.endOfFile)
                {
                    LOGGER.info("Finished reading file " + fileName);
                    pixelsReader.close();
                    break;
                }
                batch += 1;
            }

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        res += "scanFile can store the result here";
        return "success";
    }

    /**
     * for each file to scan, this method creates a thread which creates one pixelsReader to read one file
     * @param files files to scan
     * @param colsArr columns to scan
     * @throws InterruptedException
     */
    public void readersReadFiles(String[] files, String[] colsArr) {
        this.res +="store";
        for (int j=0; j<files.length; j++) {
            int finalJ = j;
            //Thread thread = new Thread(()->scanFile(files[finalJ], 1024, colsArr));
            threadPool.submit(()->scanFile(files[finalJ], 1024, colsArr));
        }
        // wait for all threads to finish, then we finished reading all files
//        for (Thread t:threads) {
//            try {
//                t.join();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }

    private PixelsReader getReader(String fileName)
    {
        PixelsReader pixelsReader = null;
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
            PixelsReaderImpl.Builder builder = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(fileName)
                    .setEnableCache(false)
                    .setCacheOrder(new ArrayList<>())
                    .setPixelsCacheReader(null)
                    .setPixelsFooterCache(new PixelsFooterCache());
            pixelsReader = builder.build();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return pixelsReader;
    }

}
