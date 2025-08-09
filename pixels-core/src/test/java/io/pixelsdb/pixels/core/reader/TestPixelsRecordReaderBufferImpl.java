package io.pixelsdb.pixels.core.reader;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
public class TestPixelsRecordReaderBufferImpl {

    @Test
    public void prepareRetinaBuffer() {

    }


    @Test
    public void testReadBatch() throws RetinaException, IOException {

        Storage storage = StorageFactory.Instance().getStorage("minio");
        String schemaName = "pixels_tpch";
        String tableName = "test";


        RetinaService retinaService = RetinaService.Instance();
        RetinaProto.GetWriterBufferResponse superVersion = retinaService.getWriterBuffer(schemaName, tableName);
        TypeDescription typeDescription = null;


        PixelsReaderOption option = new PixelsReaderOption();
        option.transId(100)
                .transTimestamp(100000);

        com.google.protobuf.ByteString byteString = superVersion.getData();
        PixelsRecordReaderBufferImpl recordReaderBuffer = new PixelsRecordReaderBufferImpl(
                option,
                byteString.toByteArray(), superVersion.getIdsList(),
                superVersion.getBitmapsList(),
                storage,
                schemaName, tableName,
                typeDescription
        );
    }
}