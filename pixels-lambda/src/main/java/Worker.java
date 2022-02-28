import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Handler value: example.Handler
public class Worker implements RequestHandler<Map<String,Object>, String>
{
    //Gson gson = new GsonBuilder().setPrettyPrinting().create();
    @Override
    public String handleRequest(Map<String,Object> event, Context context)
    {
        // aws lambda auto parse json to Map<String,Object>
        String fileName = event.get("fileName").toString();
        String[] cols = (String[]) event.get("cols");
        scanFile(fileName, 1024, cols);
        String response = new String("200 OK");
        return response;
    }

    public String scanFile(String fileName, int batchSize, String[] cols)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;
        //elementSize = 0;
        try (PixelsReader pixelsReader = getReader(fileName);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            TypeDescription schema = pixelsReader.getFileSchema();
            List<TypeDescription> colTypes =  schema.getChildren();
            List<String> fieldNames = schema.getFieldNames();
            System.out.println(colTypes);
            System.out.println(fieldNames);
//            while (true)
//            {
//                rowBatch = recordReader.readBatch(batchSize);
//                LongColumnVector acv = (LongColumnVector) rowBatch.cols[0];
//                DoubleColumnVector bcv = (DoubleColumnVector) rowBatch.cols[1];
////                DoubleColumnVector ccv = (DoubleColumnVector) rowBatch.cols[2];
////                TimestampColumnVector dcv = (TimestampColumnVector) rowBatch.cols[3];
////                LongColumnVector ecv = (LongColumnVector) rowBatch.cols[4];
////                BinaryColumnVector zcv = (BinaryColumnVector) rowBatch.cols[5];
//                if (rowBatch.endOfFile)
//                {
//                    break;
//                }
//            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return "success";
    }

    private PixelsReader getReader(String fileName)
    {
        PixelsReader pixelsReader = null;
//        String filePath = Objects.requireNonNull(
//                this.getClass().getClassLoader().getResource("files/" + fileName)).getPath();
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(fileName)
                    .setEnableCache(false)
                    .setCacheOrder(new ArrayList<>())
                    .setPixelsCacheReader(null)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
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
}
