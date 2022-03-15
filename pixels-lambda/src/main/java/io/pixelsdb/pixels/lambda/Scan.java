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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Scan {
    String res = "";
    private static final Logger LOGGER = LogManager.getLogger(Scan.class);

    /**
     * this function implements the scan operator. It invokes numWorker workers each scan one or more files
     * to finishing scan all files. Scanned results are written to S3. This functions then read scan results
     * on s3.
     * @param numFilePerWorker number of files each worker is responsible for reading
     * @param files list of file paths on s3 to scan
     */
    public void scan(int numFilePerWorker, ArrayList<String> files, ArrayList<String> cols) {
        String[] colsArr = cols.toArray(new String[0]);

        int numWorker = (int) Math.ceil((double) files.size() / numFilePerWorker);
        LambdaClient lambdaClient = LambdaClient.builder()
                .region(Region.US_EAST_2)
                .build();

        // invoke numWorker workers to each scan one or more files
        CompletableFuture<InvokeResponse>[] lambdaFutures = new CompletableFuture[numWorker];
        for (int i=0; i<numWorker; i++) {
            List<String> filesForThisWorker;
            if (i<numWorker-1) {
                filesForThisWorker = files.subList(i*numFilePerWorker, (i+1)*numFilePerWorker);
            } else {
                // for last worker, handled file might be less than other workers
                filesForThisWorker = files.subList(i*numFilePerWorker, files.size());
            }
            lambdaFutures[i] = CompletableFuture.supplyAsync(()->invokeLambda(lambdaClient, "Worker", filesForThisWorker, cols));
            System.out.println("start " + i + "th invocation");
        }

        // keep checking if any invocation's response is available
        // if a response is available, create a new thread for each file in the response to read it
        boolean[] workersResponseHandled = new boolean[numWorker];
        for (int i=0; i<numWorker; i++) {
            workersResponseHandled[i] = false;
        }
        boolean responsesAllHandled = false;
        while (!responsesAllHandled) {
            //System.out.println(Arrays.toString(workersResponseHandled));
            // check if there's worker's response ready to be handled
            for (int i=0; i<numWorker; i++) {
                if (lambdaFutures[i].isDone() && !workersResponseHandled[i]) {
                    // handle the response for this worker
                    try {
                        InvokeResponse response = lambdaFutures[i].get();
                        String resVal = response.payload().asUtf8String();
                        System.out.println("resVal: "+ resVal);
                        System.out.println("now use these s3 links to read the results read by the lambda worker");
                        // for each s3file create a thread to read it
                        resVal = resVal.replace("\"",""); // delete the "" in response as its json string
                        String[] s3files = resVal.split(",");
                        for (int j=0; j<s3files.length; j++) s3files[j] = "tiannan-test/" + s3files[j];
                        System.out.println("s3files: " + Arrays.toString(s3files));
                        readersReadFiles(s3files, colsArr);
//                        Thread[] threads = new Thread[s3files.length];
//                        for (int j=0; j<s3files.length; j++) {
//                            s3files[j] = "tiannan-test/" + s3files[j];
//                            int finalJ = j;
//                            System.out.println("before scan file");
//                            threads[j] = new Thread(()->scanFile(s3files[finalJ], 1024, colsArr));
//                        }
//                        for (Thread t:threads) t.start();
//                        // wait for all threads to finish, then we finished handling this response
//                        for (Thread t:threads) t.join();
                        workersResponseHandled[i] = true;
                    } catch (Exception e) {
                        System.out.println("response" + i + "not ready");
                    }
                }
            }
            responsesAllHandled = true;
            for (boolean handled:workersResponseHandled) {
                if (!handled) {
                    responsesAllHandled = false;
                    break;
                }
            }
        }
        System.out.println("before exiting scan");
    }

    /**
     * invoke a lambda worker instance which is responsible for reading a list of file
     * @param lambdaClient
     * @param functionName
     * @return
     */
    public InvokeResponse invokeLambda(LambdaClient lambdaClient, String functionName, List<String> filesToScan, ArrayList<String> cols) {
        ArrayList<String> arrListFiles = new ArrayList<>();
        arrListFiles.addAll(filesToScan);
        InvokeResponse res = null ;
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

        VectorizedRowBatch rowBatch;

        try (PixelsReader pixelsReader = getReader(fileName);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            LOGGER.info("Reading lambda worker result file: " + fileName);
            TypeDescription allSchema = pixelsReader.getFileSchema();
            List<TypeDescription> allColTypes =  allSchema.getChildren();
            List<String> fieldNames = allSchema.getFieldNames();
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
                // TODO log out the content of the row batches
                if (rowBatch.endOfFile)
                {
                    System.out.println("finished reading lambda worker result file " + fileName);
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
    public void readersReadFiles(String[] files, String[] colsArr) throws InterruptedException {
        Thread[] threads = new Thread[files.length];
        for (int j=0; j<files.length; j++) {
            int finalJ = j;
            System.out.println("before scan file");
            threads[j] = new Thread(()->scanFile(files[finalJ], 1024, colsArr));
        }
        for (Thread t:threads) t.start();
        // wait for all threads to finish, then we finished reading all files
        for (Thread t:threads) t.join();
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
