package io.pixelsdb.pixels.worker.sleep;

import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.ObjectPath;
import io.pixelsdb.pixels.storage.s3.S3;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * List your Amazon S3 buckets.
 *
 * This code expects that you have AWS credentials set up per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
public class ListBuckets {
    public static void show() {
        S3 s3 = new S3();
        try {
            ObjectPath p = new ObjectPath("pixels-tpch/orders");
            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                    .bucket(p.bucket)
                    .prefix(p.key);
            ListObjectsV2Request request = builder.build();
            CompletableFuture<ListObjectsV2Response> future = s3.getAsyncClient().listObjectsV2(request);
            ListObjectsV2Response response = future.get();
            System.out.println(Joiner.on('\n').join(response.contents()));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
