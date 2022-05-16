package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.DateUtil;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.load.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class RetinaWriter {

    MemoryMappedFile memoryMappedFile;
    MetadataService metadataService;

    private Properties prop;
    private Config config;

    HashMap<String, TypeDescription> schemas;

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

    RetinaWriter() {
        try {
            memoryMappedFile = new MemoryMappedFile("/dev/shm/flush", 200 * 1024);
        } catch (Exception e) {
            e.printStackTrace();
        }

        ConfigFactory configFactory = ConfigFactory.Instance();
        prop = new Properties();
        prop.setProperty("pixel.stride", configFactory.getProperty("pixel.stride"));
        prop.setProperty("row.group.size", configFactory.getProperty("row.group.size"));
        prop.setProperty("block.size", configFactory.getProperty("block.size"));
        prop.setProperty("block.replication", configFactory.getProperty("block.replication"));

        String metadataHost = ConfigFactory.Instance().getProperty("metadata.server.host");
        int metadataPort = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));
        metadataService = new MetadataService(metadataHost, metadataPort);
    }

    TypeDescription getSchema(String schemaName, String tableName) throws MetadataException {
        String key = schemaName + ":" + tableName;
        TypeDescription schema = schemas.getOrDefault(key, null);
        if (schema == null) {
            List<Column> columns = metadataService.getColumns(schemaName, tableName);
            StringBuilder builder = new StringBuilder();
            builder.append("struct<");
            String prefix = "struct<";
            for (Column col : columns) {
                builder.append(prefix);
                builder.append(col.getName());
                builder.append(":");
                builder.append(col.getType());
                prefix = ",";
            }
            builder.append(">");
            String schemaStr = builder.toString();
            schema = TypeDescription.fromString(schemaStr);
            schemas.put(key, schema);
        }
        return TypeDescription.fromString("struct<a:long,b:long,c:long,d:long,e:long,f:string,g:string,version:long>");
    }


    /**
     * Read from shared memory and write to file
     */
    void readAndWrite(String schemaName, String tableName, int rgid, long pos, String filePath) throws IOException, MetadataException {
        System.out.printf("Request received: %s %s %d %d %s...\n", schemaName, tableName, rgid, pos, filePath);
        int colNum = memoryMappedFile.getInt(pos);
        pos += Integer.BYTES;
        System.out.printf("col num: %d\n", colNum);

        int rowNum = memoryMappedFile.getInt(pos);
        pos += Integer.BYTES;
        System.out.printf("row num: %d\n", rowNum);

//        String targetDirPath = config.getPixelsPath();
//       // String schemaStr = config.getSchema();
//        int[] orderMapping = config.getOrderMapping();
//        int maxRowNum = config.getMaxRowNum();
//        String regex = config.getRegex();

        String targetDirPath = "/tmp/pixels/";

        int pixelStride = Integer.parseInt(prop.getProperty("pixel.stride"));
        int rowGroupSize = Integer.parseInt(prop.getProperty("row.group.size")) ;
        long blockSize = Long.parseLong(prop.getProperty("block.size"));
        short replication = Short.parseShort(prop.getProperty("block.replication"));

        System.out.printf("prop: %d %d\n", pixelStride, rowGroupSize);

        TypeDescription schema = getSchema(schemaName, tableName);
        VectorizedRowBatch rowBatch = schema.createRowBatch(rowNum);
        Storage targetStorage = StorageFactory.Instance().getStorage("file");
        System.out.println("here");
        String targetFilePath = targetDirPath + filePath + '|' + DateUtil.getCurTime() + ".pxl";
        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setStorage(targetStorage)
                .setPath(targetFilePath)
                .setBlockSize(blockSize)
                .setReplication(replication)
                .setBlockPadding(true)
                .setEncoding(true)
                .setCompressionBlockSize(1)
                .build();

        for (int i = 0; i < colNum; i++) {
            System.out.println(i);
            short type = memoryMappedFile.getShort(pos);
            pos += Short.BYTES;
            System.out.printf("type: %d\n", type);

            ValueType valueType = ValueType.fromShort(type);
            switch (Objects.requireNonNull(valueType)) {
                case LONG:
                    int length = memoryMappedFile.getInt(pos);
                    pos += Integer.BYTES;
                    System.out.printf("length: %d\n", length);
                    LongColumnVector longCol = (LongColumnVector) rowBatch.cols[i];
                    for (int j = 0; j < rowNum; j++) {
                        longCol.vector[j] = memoryMappedFile.getLong(pos);
                        pos += Long.BYTES;
                    }

                    break;

                case BYTES:
                    int lenLength = memoryMappedFile.getInt(pos);
                    pos += Integer.BYTES;
                    System.out.printf("length: %d\n", lenLength);
                    BinaryColumnVector strCol = (BinaryColumnVector) rowBatch.cols[i];

                    for (int j = 0; j < rowNum; j++) {
                        strCol.start[j] = memoryMappedFile.getInt(pos);
                        pos += Integer.BYTES;
                    }

                    for (int j = 0; j < rowNum; j++) {
                        strCol.lens[j] = memoryMappedFile.getInt(pos);
                        pos += Integer.BYTES;
                    }

                    int dataLength = memoryMappedFile.getInt(pos);
                    pos += Integer.BYTES;

                    memoryMappedFile.getBytes(pos, strCol.buffer, 0, dataLength);
                    pos += dataLength;

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

        // TODO Inform metadata service

        pixelsWriter.close();
    }
}
