import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.core.*;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

// Handler value: example.Handler
public class Worker implements RequestHandler<Map<String,ArrayList<String>>, String>
{
    private static final Logger LOGGER = LogManager.getLogger(Worker.class);
    //Gson gson = new GsonBuilder().setPrettyPrinting().create();
    @Override
    public String handleRequest(Map<String,ArrayList<String>> event, Context context)
    {
        String requestId = context.getAwsRequestId();
        // use a logger to debug
        LambdaLogger logger = context.getLogger();

        // aws lambda auto parse json to Map<String,Object>
        //TODO make worker create a thread for each file, and each thread uses a pixelsReader
        String fileName = event.get("fileNames").get(0);
        //https://stackoverflow.com/questions/4042434/converting-arrayliststring-to-string-in-java
        String[] cols = event.get("cols").toArray(new String[0]);

        scanFile(fileName, 1024, cols, logger, requestId);
        String response = requestId;
        return response;
    }

    public String scanFile(String fileName, int batchSize, String[] cols, LambdaLogger logger, String requestId)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;

        try (PixelsReader pixelsReader = getReader(fileName, logger);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            TypeDescription allSchema = pixelsReader.getFileSchema();
            List<TypeDescription> allColTypes =  allSchema.getChildren();
            List<String> fieldNames = allSchema.getFieldNames();
            System.out.println(allColTypes);
            System.out.println(fieldNames);
            TypeDescription queriedSchema = TypeDescription.createStruct();
            // for each queried col find its type
            ArrayList<TypeDescription> queriedColTypes = new ArrayList<>();
            for (int i=0; i<cols.length; i++) {
                // here assume fieldNames and colTypes are in same order
                queriedSchema.addField(cols[i], allColTypes.get(fieldNames.indexOf(cols[i])));
            }

            String s3Path = "tiannan-test/" + requestId;
            PixelsWriter pixelsWriter = getWriter(queriedSchema, s3Path);
            int batch = 0;
            // create two maps col->vecs, col->colType
//            Map<String, TypeDescription> colNameToType = new HashMap<String, TypeDescription>();
//            Map<String, ArrayList<ColumnVector>> colNameToVecs = new HashMap<String, ArrayList<ColumnVector>>();
//            for (int i=0; i<cols.length; i++) {
//                colNameToVecs.put(cols[i], new ArrayList<ColumnVector>());
//            }
//            for (int i=0; i< fieldNames.size(); i++) {
//                colNameToType.put(fieldNames.get(i), colTypes.get(i));
//            }
            // read the file batch by batch, then write the batch to s3
            // add each col vec to colnameToVec
            while (true)
            {
                LOGGER.info(" ****** batch number: " + batch + "*******");
                rowBatch = recordReader.readBatch(batchSize);
//                for (String col:cols) { // col: column name
//                    LOGGER.info(" column name: " + col);
//                    ColumnVector colVec = readColumnInBatch(col, fieldNames, colTypes, rowBatch);
////                    colNameToVecs.get(col).add(colVec);
//                }
                pixelsWriter.addRowBatch(rowBatch);
                if (rowBatch.endOfFile)
                {
                    pixelsWriter.close();
                    break;
                }
                batch += 1;
            }
            // now we've read everything we need from the file, wrap two maps and send to s3
//            ColToVecsAndTypes colToVecsAndTypes = new ColToVecsAndTypes(colNameToType, colNameToVecs);
//            writeToS3(colToVecsAndTypes, requestId);

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return "success";
    }

    private PixelsReader getReader(String fileName, LambdaLogger logger)
    {
        PixelsReader pixelsReader = null;
        try
        {
            //TODO make this dot dot dot, this was for debug
            Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
            PixelsReaderImpl.Builder builder1 = PixelsReaderImpl.newBuilder();
            PixelsReaderImpl.Builder builder2 = builder1.setStorage(storage);
            PixelsReaderImpl.Builder builder3 = builder2.setPath(fileName);
            PixelsReaderImpl.Builder builder4 = builder3.setEnableCache(false);
            PixelsReaderImpl.Builder builder5 = builder4.setCacheOrder(new ArrayList<>());
            PixelsReaderImpl.Builder builder6 = builder5.setPixelsCacheReader(null);
            PixelsReaderImpl.Builder builder7 = builder6.setPixelsFooterCache(new PixelsFooterCache());

            pixelsReader = builder7.build();

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return pixelsReader;
    }

    private PixelsWriter getWriter(TypeDescription schema, String filePath) {
        Storage storage = null;
        try {
            storage = StorageFactory.Instance().getStorage("s3");
        } catch (IOException e) {
            e.printStackTrace();
        }
        int pixelStride = 10000;
        int rowGroupSize = 256 * 1024 * 1024;
        long blockSize = 2048l * 1024l * 1024l;
        short replication = (short)1;
        PixelsWriter pixelsWriter =
                PixelsWriterImpl.newBuilder()
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

        return pixelsWriter;
    }


    // filedNames and colTypes should be in the same order, for example
    // colTypes: [bigint, bigint, char(256), boolean, date, char(256), char(256), int, varchar(256)]
    // fieldNames: [o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]
    //fixme: bug in logic in terms of getting the index in rowbatch.cols for the give col
    // not important for now as we read a batch then directly write to s3
    private ColumnVector readColumnInBatch(String fieldName, List<String> fieldNames, List<TypeDescription> colTypes, VectorizedRowBatch rowBatch) {
        // get the type of the col
        int fieldIndex = -1;
        for (int i=0; i<fieldNames.size(); i++ ) {
            if (fieldNames.get(i).equals(fieldName)) {
                fieldIndex = i;
            }
        }
        assert(fieldIndex>=0): "ERROR: field not found !!";
        TypeDescription colTypeDes = colTypes.get(fieldIndex);
        String colType = colTypeDes.toString();

        // read the col
        ColumnVector resultVec = rowBatch.cols[fieldIndex];
        // delay this cast when have to. But for now write here just for examining content
        // later move this code e.g. before passing to Presto
        LOGGER.info("colType: " + colType);
        LOGGER.info("type scale: " + colTypeDes.getScale());
        LOGGER.info("type precision: " + colTypeDes.getPrecision());
        if (colType.equals("bigint") || colType.equals("int")) {  // TODO should int be mapped to long vec
            //LOGGER.info( Arrays.toString(((LongColumnVector) resultVec).vector) );
        } else if (colType.contains("char(")) {
            // if type is char or varchar
            //LOGGER.info(  Arrays.toString( ((BinaryColumnVector) resultVec).vector )  );
        } else if (colType.equals("boolean")) {
            //LOGGER.info( Arrays.toString(((ByteColumnVector) resultVec).vector) );
        } else if (colType.equals("date")) {
            //LOGGER.info(Arrays.toString(((DateColumnVector) resultVec).dates));
        } else if (colType.contains("decimal")) {
            //LOGGER.info(Arrays.toString(((DecimalColumnVector) resultVec).vector));
        } else if (colType.equals("double") || colType.equals("float") ) {
            //LOGGER.info(Arrays.toString(((DoubleColumnVector) resultVec).vector));
        } else if (colType.equals("time")) {
            //LOGGER.info(Arrays.toString(((TimeColumnVector) resultVec).times));
        } else  {
            //LOGGER.error("UNKNOWN TYPE: " + colType);
        }
        return resultVec;
    }

    // Convert object to byte[]
    public static byte[] convertObjectToBytes(Object obj) {
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (ObjectOutputStream ois = new ObjectOutputStream(boas)) {
            ois.writeObject(obj);
            return boas.toByteArray();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        throw new RuntimeException();
    }

    //TODO: need to decide how much to write to s3.
    public void writeToS3(Object obj, String requestId) {
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
