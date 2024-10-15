package io.pixelsdb.pixels.example.core;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.exception.PixelsWriterException;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;

import static io.pixelsdb.pixels.core.predicate.PixelsPredicate.TRUE_PREDICATE;

public class TestBooleanColumnReader
{
    public static void main(String[] args) throws IOException
    {
        String pixelsFile = "/home/pixels/data/tpch_5g/test/test.pxl";
        Storage storage = StorageFactory.Instance().getStorage("file");
        String schemaStr = "struct<a:boolean,b:long>";

        try
        {
//            // write pixel file
//            TypeDescription schema = TypeDescription.fromString(schemaStr);
//            VectorizedRowBatch rowBatch = schema.createRowBatch();
//            ByteColumnVector a = (ByteColumnVector) rowBatch.cols[0]; // boolean
//            LongColumnVector b = (LongColumnVector) rowBatch.cols[1]; // long
//
//            PixelsWriter pixelsWriter =
//                    PixelsWriterImpl.newBuilder()
//                            .setSchema(schema)
//                            .setPixelStride(10000)
//                            .setRowGroupSize(64 * 1024 * 1024)
//                            .setStorage(storage)
//                            .setPath(pixelsFile)
//                            .setBlockSize(256 * 1024 * 1024)
//                            .setReplication((short) 3)
//                            .setBlockPadding(true)
//                            .setEncodingLevel(EncodingLevel.EL2)
//                            .setCompressionBlockSize(1)
//                            .build();
//
//            for (int i = 0; i < 100; i++)
//            {
//                int row = rowBatch.size++;
//                a.vector[row] = (byte) (i % 2);
//                a.isNull[row] = false;
//                b.vector[row] = i;
//                b.isNull[row] = false;
//                if (rowBatch.size == rowBatch.getMaxSize())
//                {
//                    pixelsWriter.addRowBatch(rowBatch);
//                    rowBatch.reset();
//                }
//            }
//            if (rowBatch.size != 0)
//            {
//                pixelsWriter.addRowBatch(rowBatch);
//                System.out.println("A rowBatch of size " + rowBatch.size + " has been written to " + pixelsFile);
//                rowBatch.reset();
//            }
//            pixelsWriter.close();

            // read pixel file
            PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(pixelsFile)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
            String[] cols = new String[1];
            cols[0] = reader.getFileSchema().getFieldNames().get(0);
            PixelsReaderOption option = new PixelsReaderOption();
            option.transId(0);
            option.timestamp(50);
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            option.predicate(TRUE_PREDICATE);
            PixelsRecordReader recordReader = reader.read(option);
            int batchSize = 10;
            VectorizedRowBatch resultBatch;
            int len = 0;
            int numRows = 0;
            int numBatches = 0;
            while (true) {
                resultBatch = recordReader.readBatch(batchSize);
                System.out.println("rowBatch: " + resultBatch);
                numBatches++;
                String result = resultBatch.toString();
                len += result.length();
                System.out.println("loop:" + numBatches + ", rowBatchSize:" + resultBatch.size);
                if (resultBatch.endOfFile) {
                    numRows += resultBatch.size;
                    break;
                }
                numRows += resultBatch.size;
            }
            reader.close();
        } catch (IOException | PixelsWriterException e)
        {
            e.printStackTrace();
        }
    }
}
