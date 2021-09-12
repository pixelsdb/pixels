package io.pixelsdb.pixels.common;

import io.pixelsdb.pixels.common.physical.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Created at: 9/8/21
 * Author: hank
 */
public class TestS3
{
    @Test
    public void testS3Writer() throws IOException
    {
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(Storage.Scheme.s3, "pixels-01/object-4",
                0, (short) 1, false);
        ByteBuffer buffer = ByteBuffer.allocate(10240);
        buffer.putLong(1);
        writer.append(buffer);
        writer.flush();
        writer.close();
    }

    @Test
    public void testS3Reader() throws IOException
    {
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(Storage.Scheme.s3, "pixels-01/object-4");
        CompletableFuture<ByteBuffer> future = reader.readAsync(8);
        future.whenComplete((resp, err) ->
        {
            if (resp != null)
            {
                System.out.println(resp.getLong());
            }
            else
            {
                err.printStackTrace();
            }
        });
        future.join();
    }
}
