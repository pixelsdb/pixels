/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * use the singleton pattern to manager data resources in the retina service.
 */
public class RetinaResourceManager
{
    private static final Logger logger = LogManager.getLogger(RetinaResourceManager.class);
    private final MetadataService metadataService;
    private final Map<String, RGVisibility> rgVisibilityMap;
    private final Map<String, PixelsWriterBuffer> pixelsWriterBufferMap;

    private RetinaResourceManager()
    {
        this.metadataService = MetadataService.Instance();
        this.rgVisibilityMap = new ConcurrentHashMap<>();
        this.pixelsWriterBufferMap = new ConcurrentHashMap<>();
    }

    private static RetinaResourceManager instance = null;

    public static RetinaResourceManager Instance()
    {
        if (instance == null)
        {
            instance = new RetinaResourceManager();
        }
        return instance;
    }

    public void addVisibility(String filePath) throws RetinaException
    {
        try
        {
            Storage storage = StorageFactory.Instance().getStorage(filePath);
            PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(storage, filePath);
            long fileLen = fsReader.getFileLength();
            fsReader.seek(fileLen - Long.BYTES);
            long fileTailOffset = fsReader.readLong(ByteOrder.BIG_ENDIAN);
            int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
            fsReader.seek(fileTailOffset);
            ByteBuffer fileTailBuffer = fsReader.readFully(fileTailLength);
            PixelsProto.FileTail fileTail = PixelsProto.FileTail.parseFrom(fileTailBuffer);
            PixelsProto.Footer footer = fileTail.getFooter();
            long fileId = this.metadataService.getFileId(filePath);
            for (int rgId = 0; rgId < footer.getRowGroupInfosCount(); rgId++)
            {
                int recordNum = footer.getRowGroupInfos(rgId).getNumberOfRows();
                RGVisibility rgVisibility = new RGVisibility(recordNum);
                String rgKey = fileId + "_" + rgId;
                rgVisibilityMap.put(rgKey, rgVisibility);
            }
        } catch (Exception e)
        {
            throw new RetinaException("Error while adding visibility", e);
        }
    }

    public long[] queryVisibility(long fileId, int rgId, long timestamp) throws RetinaException
    {
        RGVisibility rgVisibility = checkRGVisibility(fileId, rgId);
        long[] visibilityBitmap = rgVisibility.getVisibilityBitmap(timestamp);
        if (visibilityBitmap == null)
        {
            logger.error("Error while getting visibility bitmap");
            throw new RetinaException("Error while getting visibility bitmap");
        }
        return visibilityBitmap;
    }

    public void reclaimVisibility(long fileId, int rgId, long timestamp) throws RetinaException
    {
        RGVisibility rgVisibility = checkRGVisibility(fileId, rgId);
        rgVisibility.getVisibilityBitmap(timestamp);
    }

    public void deleteRecord(long fileId, int rgId, int rgRowId, long timestamp) throws RetinaException
    {
        RGVisibility rgVisibility = checkRGVisibility(fileId, rgId);
        rgVisibility.deleteRecord(rgRowId, timestamp);
    }

    public void deleteRecord(IndexProto.RowLocation rowLocation, long timestamp) throws RetinaException
    {
        deleteRecord(rowLocation.getFileId(), rowLocation.getRgId(), rowLocation.getRgRowId(), timestamp);
    }

    public void addWriterBuffer(String schemaName, String tableName) throws RetinaException
    {
        try
        {
            /**
             * get ordered and compact dir path
             * already been validated when adding visibility
             */
            Layout latestLayout = this.metadataService.getLatestLayout(schemaName, tableName);
            List<Path> orderedPaths = latestLayout.getOrderedPaths();
            List<Path> compactPaths = latestLayout.getCompactPaths();

            // get schema
            List<Column> columns = this.metadataService.getColumns(schemaName, tableName, false);
            List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
            List<String> columnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
            TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);

            PixelsWriterBuffer pixelsWriterBuffer = new PixelsWriterBuffer(schema, schemaName, tableName,
                    orderedPaths.get(0), compactPaths.get(0));
            String writerBufferKey = schemaName + "_" + tableName;
            pixelsWriterBufferMap.put(writerBufferKey, pixelsWriterBuffer);
        } catch (Exception e)
        {
            throw new RetinaException("Failed to add writer buffer for " + schemaName + "." + tableName, e);
        }
    }

    public void insertData(String schemaName, String tableName, byte[][] colValues, long timestamp) throws RetinaException
    {
        PixelsWriterBuffer writerBuffer = checkPixelsWriterBuffer(schemaName, tableName);
        writerBuffer.addRow(colValues, timestamp);
    }

    public SuperVersion getSuperVersion(String schemaName, String tableName) throws RetinaException
    {
        PixelsWriterBuffer writerBuffer = checkPixelsWriterBuffer(schemaName, tableName);
        return writerBuffer.getCurrentVersion();
    }

    public long[][] getWriterBufferVisibility(String schemaName, String tableName, List<Long> ids, long timestamp) throws RetinaException
    {
        /**
         * TODO: Return the visibility bitmap of a specific data block in writerBuffer.
         * 1. get writerBuffer from schemaName and tableName
         * 2. statistics on fileId and get the visibility bitmap of fileIds
         * 3. returns the corresponding bitmap based on the rowIndex range of each data block
         */
        return null;
    }

    /**
     * Check if the retina exists for the given filePath and rgId.
     *
     * @param fileId the file id.
     * @param rgId the row group id.
     * @throws RetinaException if the retina does not exist.
     */
    private RGVisibility checkRGVisibility(long fileId, int rgId) throws RetinaException
    {
        try
        {
            String retinaKey = fileId + "_" + rgId;
            RGVisibility rgVisibility = this.rgVisibilityMap.get(retinaKey);
            if (rgVisibility == null)
            {
                throw new RetinaException("Retina not found for fileId: " + fileId + " and rgId: " + rgId);
            }
            return rgVisibility;
        } catch (Exception e)
        {
            throw new RetinaException("Error while checking retina", e);
        }
    }

    /**
     * Check if the writer buffer exists for the given schema and table
     */
    private PixelsWriterBuffer checkPixelsWriterBuffer(String schema, String table) throws RetinaException
    {
        try
        {
            String writerBufferKey = schema + "_" + table;
            PixelsWriterBuffer writerBuffer = this.pixelsWriterBufferMap.get(writerBufferKey);
            if (writerBuffer == null)
            {
                throw new RetinaException("Writer buffer not found for schema: " + schema + " and table: " + table);
            }
            return writerBuffer;
        } catch (Exception e)
        {
            throw new RetinaException("Error while checking writer buffer", e);
        }
    }
}
