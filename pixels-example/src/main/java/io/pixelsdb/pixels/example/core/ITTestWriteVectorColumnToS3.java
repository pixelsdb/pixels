package io.pixelsdb.pixels.example.core;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.vector.*;

import java.io.IOException;
import java.sql.Timestamp;

public class ITTestWriteVectorColumnToS3 {

    public static void main(String[] args) throws IOException
    {
        // Note you may need to restart intellij to let it pick up the updated environment variable value
        // example path: s3://bucket-name/test-file.pxl
        String pixelsFile = System.getenv("PIXELS_S3_TEST_BUCKET_PATH") + "test-vec-larger2.pxl";
        Storage storage = StorageFactory.Instance().getStorage("s3");

        int dimension = 256;
        String schemaStr = String.format("struct<v:vector(%s)>", dimension);

        try
        {
            TypeDescription schema = TypeDescription.fromString(schemaStr);
            VectorizedRowBatch rowBatch = schema.createRowBatch();
            VectorColumnVector v = (VectorColumnVector) rowBatch.cols[0];

            PixelsWriter pixelsWriter =
                    PixelsWriterImpl.newBuilder()
                            .setSchema(schema)
                            .setPixelStride(10000)
                            .setRowGroupSize(64 * 1024 * 1024)
                            .setStorage(storage)
                            .setPath(pixelsFile)
                            .setBlockSize(256 * 1024 * 1024)
                            .setReplication((short) 3)
                            .setBlockPadding(true)
                            .setEncodingLevel(EncodingLevel.EL2)
                            .setCompressionBlockSize(1)
                            .build();

            long curT = System.currentTimeMillis();
            Timestamp timestamp = new Timestamp(curT);
            for (int i = 0; i < 20; i++)
            {
                int row = rowBatch.size++;
                v.vector[row] = new double[dimension];
                for (int d=0; d<dimension; d++) {
                    v.vector[row][d] = 0.1 + i;
                }
                v.isNull[row] = false;
                if (rowBatch.size == rowBatch.getMaxSize())
                {
                    pixelsWriter.addRowBatch(rowBatch);
                    rowBatch.reset();
                }
            }

            if (rowBatch.size != 0)
            {
                pixelsWriter.addRowBatch(rowBatch);
                System.out.println("A rowBatch of size " + rowBatch.size + " has been written to " + pixelsFile);
                rowBatch.reset();
            }

            pixelsWriter.close();
        } catch (IOException | PixelsWriterException e)
        {
            e.printStackTrace();
        }
    }

}
