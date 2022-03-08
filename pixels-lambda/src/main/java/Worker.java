import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

// Handler value: example.Handler
public class Worker implements RequestHandler<Map<String,ArrayList<String>>, String>
{
    private static final Logger LOGGER = LogManager.getLogger(Worker.class);
    //Gson gson = new GsonBuilder().setPrettyPrinting().create();
    @Override
    public String handleRequest(Map<String,ArrayList<String>> event, Context context)
    {
        // use a logger to debug
        LambdaLogger logger = context.getLogger();
        if (logger!=null) logger.log("Will this be logged by aws lambda?\n");
        if (logger!=null) logger.log("**************************************");
        if (logger!=null) logger.log(event.toString());

        // aws lambda auto parse json to Map<String,Object>
        String fileName = event.get("fileName").get(0);
        //https://stackoverflow.com/questions/4042434/converting-arrayliststring-to-string-in-java
        String[] cols = event.get("cols").toArray(new String[0]);
        if (logger!=null) logger.log("fileName: "+fileName);
        if (logger!=null) logger.log("cols: "+Arrays.toString(cols));



        String fileNameForTest = "pixels-tpch-orders-v-0-order/20220306043322_0.pxl";
        String[] colsForTest = {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"};
        if (logger!=null) logger.log("before scan file");
        scanFile(fileNameForTest, 1024, colsForTest, logger);
        String response = Arrays.toString(cols);
        if (logger!=null) logger.log(response);
        return response;
    }

    public String scanFile(String fileName, int batchSize, String[] cols, LambdaLogger logger)
    {
//        if (logger!=null) logger.log("***********************");
//        if (logger!=null) logger.log("**in scaleFile()**");
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;
        //elementSize = 0;
//        if (logger!=null) logger.log("wtf: just outside try");
        try (PixelsReader pixelsReader = getReader(fileName, logger);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
//            if (logger!=null) logger.log("wtf: enter try block");
            TypeDescription schema = pixelsReader.getFileSchema();
            List<TypeDescription> colTypes =  schema.getChildren();
            List<String> fieldNames = schema.getFieldNames();
            if (logger!=null) {
                if (logger!=null) logger.log("colTypes: " + colTypes.toString());
                if (logger!=null) logger.log("fieldNames: " + fieldNames);
            }
            System.out.println(colTypes);
            System.out.println(fieldNames);
            int batch = 0;
            while (true)
            {
                LOGGER.info(" ****** batch number: " + batch + "*******");
                rowBatch = recordReader.readBatch(batchSize);
                for (String col:cols) { // col: column name
                    LOGGER.info(" column name: " + col);
                    readColumnInBatch(col, fieldNames, colTypes, rowBatch);
                }
                if (rowBatch.endOfFile)
                {
                    break;
                }
                batch += 1;
            }
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
//        String filePath = Objects.requireNonNull(
//                this.getClass().getClassLoader().getResource("files/" + fileName)).getPath();
        try
        {
            if (logger!=null) logger.log("enter getReader()");
            Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
            PixelsReaderImpl.Builder builder1 = PixelsReaderImpl.newBuilder();
            if (logger!=null) logger.log("after builder1");
            PixelsReaderImpl.Builder builder2 = builder1.setStorage(storage);
            if (logger!=null) logger.log("after builder2");
            PixelsReaderImpl.Builder builder3 = builder2.setPath(fileName);
            if (logger!=null) logger.log("after builder3");
            PixelsReaderImpl.Builder builder4 = builder3.setEnableCache(false);
            if (logger!=null) logger.log("after builder4");
            PixelsReaderImpl.Builder builder5 = builder4.setCacheOrder(new ArrayList<>());
            if (logger!=null) logger.log("after builder5");
            PixelsReaderImpl.Builder builder6 = builder5.setPixelsCacheReader(null);
            if (logger!=null) logger.log("after builder6");
            PixelsReaderImpl.Builder builder7 = builder6.setPixelsFooterCache(new PixelsFooterCache());
            if (logger!=null) logger.log("after builder7");
            pixelsReader = builder7.build();


            if (logger!=null) logger.log("getReader build pixelsReader successfully");

//            pixelsReader = PixelsReaderImpl.newBuilder()
//                    .setStorage(storage)
//                    .setPath("s3://pixels-tpch-customer-v-0-order/20220213133651_0.pxl")
//                    .build();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return pixelsReader;
    }


    // filedNames and colTypes should be in the same order, for example
    // colTypes: [bigint, bigint, char(256), boolean, date, char(256), char(256), int, varchar(256)]
    // fieldNames: [o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]
    private ColumnVector readColumnInBatch(String fieldName, List<String> fieldNames, List<TypeDescription> colTypes, VectorizedRowBatch rowBatch) {
        // get the type of the col
        int fieldIndex = -1;
        for (int i=0; i<fieldNames.size(); i++ ) {
            if (fieldNames.get(i).equals(fieldName)) {
                fieldIndex = i;
            }
        }
        assert(fieldIndex>=0): "ERROR: field not found !!";
        String colType = colTypes.get(fieldIndex).toString();

        // read the col
        ColumnVector resultVec = rowBatch.cols[fieldIndex];
        // delay this cast when have to. But for now write here just for examining content
        // later move this code e.g. before passing to Presto
        LOGGER.info("colType: " + colType);
        if (colType.equals("bigint") || colType.equals("int")) {  // TODO should int be mapped to long vec
            LOGGER.info( Arrays.toString(((LongColumnVector) resultVec).vector) );
        } else if (colType.contains("char(")) {
            // if type is char or varchar
            LOGGER.info(  Arrays.toString( ((BinaryColumnVector) resultVec).vector )  );
        } else if (colType.equals("boolean")) {
            LOGGER.info( Arrays.toString(((ByteColumnVector) resultVec).vector) );
        } else if (colType.equals("date")) {
            LOGGER.info(Arrays.toString(((DateColumnVector) resultVec).dates));
        } else if (colType.equals("double") || colType.equals("float")) {
            LOGGER.info(Arrays.toString(((DoubleColumnVector) resultVec).vector));
        } else if (colType.equals("time")) {
            LOGGER.info(Arrays.toString(((TimeColumnVector) resultVec).times));
        } else  {
            LOGGER.error("UNKNOWN TYPE: " + colType);
        }
        return resultVec;
    }
}
