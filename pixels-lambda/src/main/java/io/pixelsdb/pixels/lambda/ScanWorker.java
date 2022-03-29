/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * response is a list of files read and then written to s3
 */
public class ScanWorker implements RequestHandler<Map<String, ArrayList<String>>, String>
{
    // slf4j is used in the official example java-blank.
    private static final Logger logger = LoggerFactory.getLogger(ScanWorker.class);

    @Override
    public String handleRequest(Map<String, ArrayList<String>> event, Context context)
    {
        ExecutorService threadPool = Executors.newFixedThreadPool(8);
        logger.info("enter handleRequest");
        long lambdaStartTime = System.nanoTime();
        String requestId = context.getAwsRequestId();

        // each worker create a thread for each file, and each thread uses a pixelsReader
        ArrayList<String> fileNames = event.get("fileNames");
        //https://stackoverflow.com/questions/4042434/converting-arrayliststring-to-string-in-java
        String[] cols = event.get("cols").toArray(new String[0]);

        // for each file to read, create a thread which uses a reader to read one file and writes the results to s3
        // Thread[] threads = new Thread[fileNames.size()];
        logger.debug("start submitting tasks to thread pool");
        for (int i = 0; i < fileNames.size(); i++)
        {
            int finalI = i;
            threadPool.execute(() -> scanFile(fileNames.get(finalI), 1024, cols, requestId + "file" + finalI));
            // runnables[i] = () -> scanFile(fileNames.get(finalI), 1024, cols, requestId+"file"+finalI);
        }
        threadPool.shutdown();
        try
        {
            threadPool.awaitTermination(300, TimeUnit.SECONDS);//TODO maybe threadpool shouldn't be class method as that might be shared between lambda instances. and shut down one threadpool would shut down all? maybe after shutdown can restart?
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        logger.debug("thread pool shut down");


        // create response to inform invoker which are the s3 paths of files written
        String response = "";
        for (int i = 0; i < fileNames.size(); i++)
        {
            if (i < fileNames.size() - 1)
            {
                response = response + requestId + "file" + i + ",";
            } else
            {
                response = response + requestId + "file" + i;
            }
        }
        long lambdaEndTime = System.nanoTime();
        double lambdaDurationMs = 1.0 * (lambdaEndTime - lambdaStartTime) / Math.pow(10, 6);
        logger.debug("lambda requestid " + requestId + " duration: " + lambdaDurationMs);
        return response;
    }

    /**
     * @param fileName
     * @param batchSize
     * @param cols
     * @param resultFile fileName on s3 to store pixels readers' results
     * @return
     */
    public String scanFile(String fileName, int batchSize, String[] cols, String resultFile)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;

        try (PixelsReader pixelsReader = getReader(fileName);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            logger.debug("start scan file: " + fileName);
            TypeDescription allSchema = pixelsReader.getFileSchema();
            List<TypeDescription> allColTypes = allSchema.getChildren();
            List<String> fieldNames = allSchema.getFieldNames();
            System.out.println(allColTypes);
            System.out.println(fieldNames);
            TypeDescription queriedSchema = TypeDescription.createStruct();
            // for each queried col find its type
            ArrayList<TypeDescription> queriedColTypes = new ArrayList<>();
            for (int i = 0; i < cols.length; i++)
            {
                // here assume fieldNames and colTypes are in same order
                queriedSchema.addField(cols[i], allColTypes.get(fieldNames.indexOf(cols[i])));
            }

            String s3Path = "tiannan-test/" + resultFile;
            PixelsWriter pixelsWriter = getWriter(queriedSchema, s3Path);
            int batch = 0;

            while (true)
            {
                rowBatch = recordReader.readBatch(batchSize);
                pixelsWriter.addRowBatch(rowBatch);
                if (rowBatch.endOfFile)
                {
                    pixelsReader.close();
                    pixelsWriter.close();
                    break;
                }
                batch += 1;
            }
        } catch (IOException e)
        {
            e.printStackTrace();
            logger.error("failed to scan", e);
        }
        logger.debug("finish scanning file: " + fileName);
        return "success";
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

        } catch (IOException e)
        {
            e.printStackTrace();
        }

        return pixelsReader;
    }

    private PixelsWriter getWriter(TypeDescription schema, String filePath)
    {
        logger.debug("try to create storage.");
        Storage storage = null;
        try
        {
            storage = StorageFactory.Instance().getStorage("s3");
        } catch (IOException e)
        {
            e.printStackTrace();
            logger.error("failed to get storage", e);
        }
        int pixelStride = 10000;
        int rowGroupSize = 256 * 1024 * 1024;
        long blockSize = 2048l * 1024l * 1024l;
        short replication = (short) 1;
        logger.debug("start create writer");
        PixelsWriter pixelsWriter = null;
        try
        {
            pixelsWriter = PixelsWriterImpl.newBuilder()
                    .setSchema(schema)
                    .setPixelStride(pixelStride)
                    .setRowGroupSize(rowGroupSize)
                    .setStorage(storage)
                    .setFilePath(filePath)
                    .setBlockSize(blockSize)
                    .setReplication(replication)
                    .setBlockPadding(true)
                    .setEncoding(true)
                    .setCompressionBlockSize(1)
                    .build();
        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error("failed to create writer", e);
        }

        return pixelsWriter;
    }

    // filedNames and colTypes should be in the same order, for example
    // colTypes: [bigint, bigint, char(256), boolean, date, char(256), char(256), int, varchar(256)]
    // fieldNames: [o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]
    //fixme: bug in logic in terms of getting the index in rowbatch.cols for the give col
    // not important for now as we read a batch then directly write to s3
    private ColumnVector readColumnInBatch(String fieldName, List<String> fieldNames, List<TypeDescription> colTypes, VectorizedRowBatch rowBatch)
    {
        // get the type of the col
        int fieldIndex = -1;
        for (int i = 0; i < fieldNames.size(); i++)
        {
            if (fieldNames.get(i).equals(fieldName))
            {
                fieldIndex = i;
            }
        }
        assert (fieldIndex >= 0) : "ERROR: field not found !!";
        TypeDescription colTypeDes = colTypes.get(fieldIndex);
        String colType = colTypeDes.toString();

        // read the col
        ColumnVector resultVec = rowBatch.cols[fieldIndex];
        // delay this cast when have to. But for now write here just for examining content
        // later move this code e.g. before passing to Presto
        logger.info("colType: " + colType);
        logger.info("type scale: " + colTypeDes.getScale());
        logger.info("type precision: " + colTypeDes.getPrecision());
        if (colType.equals("bigint") || colType.equals("int"))
        {  // TODO should int be mapped to long vec
            //logger.info( Arrays.toString(((LongColumnVector) resultVec).vector) );
        } else if (colType.contains("char("))
        {
            // if type is char or varchar
            //logger.info(  Arrays.toString( ((BinaryColumnVector) resultVec).vector )  );
        } else if (colType.equals("boolean"))
        {
            //logger.info( Arrays.toString(((ByteColumnVector) resultVec).vector) );
        } else if (colType.equals("date"))
        {
            //logger.info(Arrays.toString(((DateColumnVector) resultVec).dates));
        } else if (colType.contains("decimal"))
        {
            //logger.info(Arrays.toString(((DecimalColumnVector) resultVec).vector));
        } else if (colType.equals("double") || colType.equals("float"))
        {
            //logger.info(Arrays.toString(((DoubleColumnVector) resultVec).vector));
        } else if (colType.equals("time"))
        {
            //logger.info(Arrays.toString(((TimeColumnVector) resultVec).times));
        } else
        {
            //logger.error("UNKNOWN TYPE: " + colType);
        }
        return resultVec;
    }

    // Convert object to byte[]
    public static byte[] convertObjectToBytes(Object obj)
    {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (ObjectOutputStream ois = new ObjectOutputStream(boas))
        {
            ois.writeObject(obj);
            return boas.toByteArray();
        } catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
        throw new RuntimeException();
    }

    //TODO: need to decide how much to write to s3.
    public void writeToS3(Object obj, String requestId)
    {
        S3Client s3 = S3Client.builder()
                .region(Region.US_WEST_2)
                .build();
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket("tiannan-test")
                .key(requestId)
                .build();
        s3.putObject(objectRequest, RequestBody.fromBytes(convertObjectToBytes(obj)));
    }
}
