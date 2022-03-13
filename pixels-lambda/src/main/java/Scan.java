import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Scan {

    /**
     * this function implements the scan operator. It invokes numWorker workers each scan one or more files
     * to finishing scan all files
     * @param numFilePerWorker number of files each worker is responsible for reading
     * @param files list of file paths on s3 to scan
     */
    public void scan(int numFilePerWorker, ArrayList<String> files, ArrayList<String> cols) {

        int numWorker = (int) Math.ceil((double) files.size() / numFilePerWorker);
        LambdaClient lambdaClient = LambdaClient.builder()
                .region(Region.US_EAST_2)
                .build();

        // invoke numWorker workers to each scan one or more files
        CompletableFuture<InvokeResponse>[] lambdaFutures = new CompletableFuture[numWorker];
        for (int i=0; i<numWorker; i++) {
            // TODO check off by one error
            List<String> filesForThisWorker;
            if (i<numWorker-1) {
                filesForThisWorker = files.subList(i*numFilePerWorker, (i+1)*numFilePerWorker);
            } else {
                // for last worker, handled file might be less than other workers
                filesForThisWorker = files.subList(i*numFilePerWorker, files.size());
            }
            lambdaFutures[i] = CompletableFuture.supplyAsync(()->invokeLambda(lambdaClient, "Worker", filesForThisWorker, cols));
            System.out.println("start " + i + "th invocation");
            //invokeLambda(lambdaClient, "Worker");
        }

        // keep checking if any invocation's response is available
        boolean[] workersResponseSent = new boolean[numWorker];
        for (int i=0; i<numWorker; i++) {
            workersResponseSent[i] = false;
        }
        boolean allSent = false;
        while (!allSent) {
            for (int i=0; i<numWorker; i++) {
                if (lambdaFutures[i].isDone() && !workersResponseSent[i]) {
                    try {
                        InvokeResponse response = lambdaFutures[i].get();
                        String resVal = response.payload().asUtf8String();
                        System.out.println("resVal: "+ resVal);
                        System.out.println("now use this s3 link to read the results read by the lambda worker");
                        workersResponseSent[i] = true;
                    } catch (Exception e) {
                        System.out.println("response" + i + "not ready");
                    }
                }
            }
            allSent = true;
            for (boolean sent:workersResponseSent) {
                if (!sent) {
                    allSent = false;
                    break;
                }
            }
        }
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
            //Need a SdkBytes instance for the payload
//            JsonObject jsonObj = new JsonObject();
//            JsonArray jsonArrFiles = new Gson().toJsonTree(arrListFiles).getAsJsonArray();
//            JsonArray jsonArrCols = new Gson().toJsonTree(cols).getAsJsonArray();
//            jsonObj.add("fileName", jsonArrFiles); //TODO change to filenames
//            jsonObj.add("cols", jsonArrCols);
//            SdkBytes payload = SdkBytes.fromUtf8String(jsonObj.getAsString()) ;
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
//            String value = res.payload().asUtf8String() ;
//            System.out.println(value);

        } catch(LambdaException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return res;
    }
}
