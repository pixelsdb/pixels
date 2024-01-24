package io.pixelsdb.pixels.example.core;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;
import java.util.List;

public class ITTestReadVectorColumnFromS3 {

    public static void main(String[] args)
    {
        readVectorColumn("s3://tiannan-test/test_arr_table_4/v-0-ordered/1.pxl");
    }

    public static void readVectorColumn(String currentPath) {
        // Note you may need to restart intellij to let it pick up the updated environment variable value
        // example path: s3://bucket-name/test-file.pxl
//        String currentPath = System.getenv("PIXELS_S3_TEST_BUCKET_PATH") + "test-vec-larger2.pxl";
        System.out.println(currentPath);
        try {
            Storage storage = StorageFactory.Instance().getStorage("s3");
            PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(currentPath)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();

            TypeDescription schema = reader.getFileSchema();
            System.out.println(schema);
            List<String> fieldNames = schema.getFieldNames();
            System.out.println("fieldNames: " + fieldNames);
            String[] cols = new String[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                cols[i] = fieldNames.get(i);
            }

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(cols);
            PixelsRecordReader recordReader = reader.read(option);
            System.out.println("recordReader.getCompletedRows():" + recordReader.getCompletedRows());
            System.out.println("reader.getRowGroupInfo(0).getNumberOfRows():" + reader.getRowGroupInfo(0).getNumberOfRows());
            int batchSize = 10000;
            VectorizedRowBatch rowBatch;
            int len = 0;
            int numRows = 0;
            int numBatches = 0;
            while (true) {
                rowBatch = recordReader.readBatch(batchSize);
                System.out.println("rowBatch: " + rowBatch);
                numBatches++;
                String result = rowBatch.toString();
                len += result.length();
                System.out.println("loop:" + numBatches + ", rowBatchSize:" + rowBatch.size);
                if (rowBatch.endOfFile) {
                    numRows += rowBatch.size;
                    break;
                }
                numRows += rowBatch.size;
            }
            System.out.println("numBatches:" + numBatches + ", numRows:" + numRows);
            reader.close();
        } catch (IOException e) {
            System.out.println("Err path: " + currentPath.toString());
            e.printStackTrace();
        }
    }
}
