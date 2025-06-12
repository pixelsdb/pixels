package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;

public class TestMinIO
{
    @Test
    public void testReadWriter() throws IOException
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        ConfigMinio(configFactory.getProperty("minio.region"),
                configFactory.getProperty("minio.endpoint"),
                configFactory.getProperty("minio.access.key"),
                configFactory.getProperty("minio.secret.key"));
        Storage minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);

        String file = "test/test/0.pxl";
        // writer into minio
        assert(minio.exists(file));
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(minio, file, true);
        byte[] buffer = new byte[] {2,0, 2, 0, 2, 0, 1, 5, 4, 5};
        writer.append(buffer, 0, buffer.length);
        writer.close();

        // read from minio
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(minio, file);
        int length = (int) reader.getFileLength();
        byte[] data = new byte[length];
        reader.readFully(data, 0, length);
        System.out.println(Arrays.toString(data));
        reader.close();

        minio.delete(file, false);
        assert(minio.exists(file));
    }
}
