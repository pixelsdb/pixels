package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.load.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Properties;

public class RetinaWriter {

    MemoryMappedFile memoryMappedFile;

    private Properties prop;
    private Config config;

    {
        try {
            memoryMappedFile = new MemoryMappedFile("/dev/shm/flush", 200 * 1024);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    enum ValueType {
        NONE,  // Useful when the type of column vector has not be determined yet.
        LONG,
        DOUBLE,
        BYTES,
        DECIMAL,
        TIMESTAMP,
        INTERVAL_DAY_TIME,
        STRUCT,
        LIST,
        MAP,
        UNION;

        public static ValueType fromShort(short x) {
            switch(x) {
                case 0:
                    return NONE;
                case 1:
                    return LONG;
                case 3:
                    return BYTES;
            }
            return null;
        }
    }


    /**
     * Read from shared memory and write to file
     */
    void readAndWrite(String schemaStr, long pos, String filePath) throws IOException {
        int colNum = memoryMappedFile.getInt(pos);
        pos += Integer.BYTES;

        int rowNum = memoryMappedFile.getInt(pos);
        pos += Integer.BYTES;

        String targetDirPath = config.getPixelsPath();
       // String schemaStr = config.getSchema();
        int[] orderMapping = config.getOrderMapping();
        int maxRowNum = config.getMaxRowNum();
        String regex = config.getRegex();

        int pixelStride = Integer.parseInt(prop.getProperty("pixel.stride"));
        int rowGroupSize = Integer.parseInt(prop.getProperty("row.group.size")) * 1024 * 1024;
        long blockSize = Long.parseLong(prop.getProperty("block.size")) * 1024l * 1024l;
        short replication = Short.parseShort(prop.getProperty("block.replication"));

        TypeDescription schema = TypeDescription.fromString(schemaStr);
        VectorizedRowBatch rowBatch = schema.createRowBatch(rowNum);
        Storage targetStorage = StorageFactory.Instance().getStorage(targetDirPath);

        String targetFilePath = targetDirPath + DateUtil.getCurTime() + ".pxl";
        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setStorage(targetStorage)
                .setFilePath(targetFilePath)
                .setBlockSize(blockSize)
                .setReplication(replication)
                .setBlockPadding(true)
                .setEncoding(true)
                .setCompressionBlockSize(1)
                .build();

        for (int i = 0; i < colNum; i++) {
            short type = memoryMappedFile.getShort(pos);
            pos += Short.BYTES;

            ValueType valueType = ValueType.fromShort(type);
            switch (Objects.requireNonNull(valueType)) {
                case LONG:
                    int length = memoryMappedFile.getInt(pos);
                    pos += Integer.BYTES;

                    LongColumnVector longCol = (LongColumnVector) rowBatch.cols[i];
                    ByteBuffer buffer = memoryMappedFile.getDirectByteBuffer(pos, length);
                    pos += length;

                    longCol.vector = buffer.asLongBuffer().array();
                    break;

                case BYTES:
                    int lenLength = memoryMappedFile.getInt(pos);
                    pos += Integer.BYTES;

                    BinaryColumnVector strCol = (BinaryColumnVector) rowBatch.cols[i];

                    ByteBuffer startBuffer = memoryMappedFile.getDirectByteBuffer(pos, lenLength);
                    pos += lenLength;
                    strCol.start = startBuffer.asIntBuffer().array();

                    ByteBuffer lenBuffer = memoryMappedFile.getDirectByteBuffer(pos, lenLength);
                    pos += lenLength;
                    strCol.lens = lenBuffer.asIntBuffer().array();

                    int dataLength = memoryMappedFile.getInt(pos);
                    pos += Integer.BYTES;

                    ByteBuffer dataBuffer = memoryMappedFile.getDirectByteBuffer(pos, dataLength);
                    pos += dataLength;
                    strCol.buffer = dataBuffer.array();

                    for (int j = 0; j < rowNum; j++) {
                        strCol.vector[j] = strCol.buffer;
                    }

                    break;
            }
        }

        if (rowBatch.size != 0) {
            pixelsWriter.addRowBatch(rowBatch);
            rowBatch.reset();
        }

        pixelsWriter.close();

    }
}
