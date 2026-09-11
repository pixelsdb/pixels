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

package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPixelsRecordReaderBufferImpl
{
    private static final TypeDescription SCHEMA = TypeDescription.createSchemaFromStrings(
            Collections.singletonList("id"), Collections.singletonList("int"));

    @Before
    public void setUp()
    {
        ConfigFactory.Instance().addProperty("retina.enable", "true");
        ConfigFactory.Instance().addProperty("retina.buffer.object.storage.folder", "file:///tmp/retina-buffer/");
        ConfigFactory.Instance().addProperty("retina.reader.prefetch.threads", "2");
    }

    @Test
    public void normalizesObjectStorageFolder()
    {
        String expected = "minio://pixels/retina-buffer/";
        assertEquals(expected, PixelsRecordReaderBufferImpl.normalizeRetinaBufferStorageFolder(
                "minio://pixels/retina-buffer"));
        assertEquals(expected, PixelsRecordReaderBufferImpl.normalizeRetinaBufferStorageFolder(expected));
    }

    @Test(expected = IOException.class)
    public void rejectsMisalignedBitmaps() throws Exception
    {
        newReader(new byte[0], Collections.singletonList(10L),
                Collections.singletonList(emptyBitmap()), Collections.emptyMap(), queryOption(Long.MAX_VALUE));
    }

    @Test
    public void readsActiveMemtableRows() throws Exception
    {
        List<Integer> values = drainVisibleIds(newReader(
                serializedBatch(7, 1L),
                Collections.emptyList(),
                Collections.singletonList(emptyBitmap()),
                Collections.emptyMap(),
                queryOption(Long.MAX_VALUE)));
        assertEquals(Collections.singletonList(7), values);
    }

    @Test
    public void readsObjectRowsWhenBitmapIsEmpty() throws Exception
    {
        Map<Long, byte[]> objects = new HashMap<>();
        objects.put(10L, serializedBatch(11, 1L));
        objects.put(20L, serializedBatch(21, 1L));
        List<Integer> values = drainVisibleIds(newReader(
                new byte[0],
                Arrays.asList(10L, 20L),
                Arrays.asList(emptyBitmap(), emptyBitmap(), emptyBitmap()),
                objects,
                queryOption(Long.MAX_VALUE)));
        Collections.sort(values);
        assertEquals(Arrays.asList(11, 21), values);
    }

    @Test
    public void hidesDeletedObjectRows() throws Exception
    {
        Map<Long, byte[]> objects = new HashMap<>();
        objects.put(10L, serializedBatch(11, 1L));
        List<Integer> values = drainVisibleIds(newReader(
                new byte[0],
                Collections.singletonList(10L),
                Arrays.asList(emptyBitmap(), deletedBitmap()),
                objects,
                queryOption(Long.MAX_VALUE)));
        assertEquals(Collections.emptyList(), values);
    }

    @Test
    public void hidesRowsNewerThanQueryTimestamp() throws Exception
    {
        List<Integer> values = drainVisibleIds(newReader(
                serializedBatch(7, 20L),
                Collections.emptyList(),
                Collections.singletonList(emptyBitmap()),
                Collections.emptyMap(),
                queryOption(10L)));
        assertEquals(Collections.emptyList(), values);
    }

    @Test
    public void emptySourcesReachEndOfFile() throws Exception
    {
        PixelsRecordReaderBufferImpl reader = newReader(
                new byte[0],
                Collections.emptyList(),
                Collections.singletonList(emptyBitmap()),
                Collections.emptyMap(),
                queryOption(Long.MAX_VALUE));
        assertEquals(Collections.emptyList(), drainVisibleIds(reader));
        assertTrue(reader.isEndOfFile());
        assertEquals(0, reader.getNumReadRequests());
    }

    @Test
    public void countsActiveAndObjectPrefetchRequests() throws Exception
    {
        PixelsRecordReaderBufferImpl reader = newReader(
                serializedBatch(1, 1L),
                Arrays.asList(10L, 20L),
                Arrays.asList(emptyBitmap(), emptyBitmap(), emptyBitmap()),
                Collections.emptyMap(),
                queryOption(Long.MAX_VALUE));
        assertEquals(3, reader.getNumReadRequests());
        reader.close();
    }

    @Test
    public void outOfOrderPrefetchKeepsSourceBitmapBoundToBatch() throws Exception
    {
        Map<Long, byte[]> objects = new HashMap<>();
        objects.put(10L, serializedBatch(10, 1L));
        objects.put(20L, serializedBatch(20, 1L));
        PixelsRecordReaderBufferImpl reader = new PixelsRecordReaderBufferImpl(
                queryOption(Long.MAX_VALUE), "localhost", new byte[0], Arrays.asList(10L, 20L),
                Arrays.asList(emptyBitmap(), deletedBitmap(), emptyBitmap()),
                null, 1L, 0, SCHEMA)
        {
            @Override
            protected ByteBuffer getMemtableDataFromStorage(String path) throws IOException
            {
                if (path.endsWith("_10"))
                {
                    sleepQuietly(100);
                }
                return ByteBuffer.wrap(objects.get(objectIdFromPath(path)));
            }
        };

        List<Integer> values = drainVisibleIds(reader);
        assertEquals(Collections.singletonList(20), values);
    }

    @Test
    public void prefetchFailureSurfacesInsteadOfEof() throws Exception
    {
        PixelsRecordReaderBufferImpl reader = new PixelsRecordReaderBufferImpl(
                queryOption(Long.MAX_VALUE), "localhost", new byte[0], Collections.singletonList(10L),
                Arrays.asList(emptyBitmap(), emptyBitmap()),
                null, 1L, 0, SCHEMA)
        {
            @Override
            protected ByteBuffer getMemtableDataFromStorage(String path) throws IOException
            {
                throw new IOException("object missing: " + path);
            }
        };

        IOException failure = null;
        try
        {
            drainVisibleIds(reader);
        }
        catch (IOException e)
        {
            failure = e;
        }
        finally
        {
            reader.close();
        }
        assertTrue(failure != null);
        assertTrue(failure.getMessage().contains("object missing")
                || (failure.getCause() != null && failure.getCause().getMessage().contains("object missing")));
    }

    @Test
    public void testReadBatchAgainstLiveWriteBuffer() throws RetinaException, IOException, TransException, MetadataException
    {
        RetinaService retinaService;
        try
        {
            retinaService = RetinaService.Instance();
        }
        catch (Throwable setupFailure)
        {
            Assume.assumeNoException("requires a running Retina service", setupFailure);
            return;
        }
        Assume.assumeTrue("requires a running Retina service", retinaService.isEnabled());

        Storage storage = StorageFactory.Instance().getStorage("s3");
        String schemaName = "pixels_bench_sf10x";
        String tableName = "checkingaccount";

        MetadataService metadataService = MetadataService.Instance();
        List<Column> columns = metadataService.getColumns(schemaName, tableName, false);
        Table table = metadataService.getTable(schemaName, tableName);
        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        List<String> columnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
        TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        String[] includeCols = new String[columns.size()];
        for (int i = 0; i < includeCols.length; ++i)
        {
            includeCols[i] = columns.get(i).getName();
        }

        TransService transService = TransService.Instance();
        TransContext transContext = transService.beginTrans(true);
        long timeStamp = transContext.getTimestamp();
        long transId = transContext.getTransId();

        RetinaProto.GetWriteBufferResponse superVersion =
                retinaService.getWriteBuffer(schemaName, tableName, 0, timeStamp);

        PixelsReaderOption option = new PixelsReaderOption();
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(true);
        option.includeCols(includeCols);
        option.transId(transId);
        option.transTimestamp(timeStamp);
        String retinaServiceHost = ConfigFactory.Instance().getProperty("retina.server.host");
        byte[] activeMemtableData = superVersion.getData().toByteArray();
        assertEquals(superVersion.getBlockIdsCount() + 1, superVersion.getFileIdsCount());
        assertEquals(superVersion.getBlockIdsCount() + 1, superVersion.getBitmapsCount());
        PixelsRecordReaderBufferImpl reader = new PixelsRecordReaderBufferImpl(
                option,
                retinaServiceHost,
                activeMemtableData, superVersion.getBlockIdsList(),
                superVersion.getBitmapsList(),
                storage,
                table.getId(),
                0,
                schema);

        int expectedBatchNum = superVersion.getBlockIdsList().size();
        if (activeMemtableData.length > 0)
        {
            ++expectedBatchNum;
        }
        int readBatchNum = 0;
        while (true)
        {
            VectorizedRowBatch vectorizedRowBatch = reader.readBatch();
            if (vectorizedRowBatch.endOfFile)
            {
                break;
            }
            if (vectorizedRowBatch.isEmpty())
            {
                continue;
            }
            ++readBatchNum;
        }
        reader.close();
        assertEquals(expectedBatchNum, readBatchNum);
    }

    private static PixelsReaderOption queryOption(long timestamp)
    {
        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[] {"id"});
        option.transTimestamp(timestamp);
        return option;
    }

    private static PixelsRecordReaderBufferImpl newReader(
            byte[] activeData,
            List<Long> objectIds,
            List<RetinaProto.VisibilityBitmap> bitmaps,
            Map<Long, byte[]> objects,
            PixelsReaderOption option) throws IOException
    {
        return new PixelsRecordReaderBufferImpl(
                option, "localhost", activeData, objectIds, bitmaps, null, 1L, 0, SCHEMA)
        {
            @Override
            protected ByteBuffer getMemtableDataFromStorage(String path) throws IOException
            {
                byte[] data = objects.get(objectIdFromPath(path));
                if (data == null)
                {
                    throw new IOException("missing in-memory object for " + path);
                }
                return ByteBuffer.wrap(data);
            }
        };
    }

    private static List<Integer> drainVisibleIds(PixelsRecordReaderBufferImpl reader) throws IOException
    {
        List<Integer> values = new ArrayList<>();
        try
        {
            while (true)
            {
                VectorizedRowBatch batch = reader.readBatch();
                if (batch.endOfFile)
                {
                    break;
                }
                if (batch.size <= 0)
                {
                    continue;
                }
                IntColumnVector col = (IntColumnVector) batch.cols[0];
                for (int i = 0; i < batch.size; i++)
                {
                    values.add((int) col.vector[i]);
                }
            }
        }
        finally
        {
            reader.close();
        }
        return values;
    }

    private static RetinaProto.VisibilityBitmap emptyBitmap()
    {
        return RetinaProto.VisibilityBitmap.getDefaultInstance();
    }

    private static RetinaProto.VisibilityBitmap deletedBitmap()
    {
        return RetinaProto.VisibilityBitmap.newBuilder().addBitmap(1L).build();
    }

    private static byte[] serializedBatch(int value, long timestamp)
    {
        VectorizedRowBatch batch = SCHEMA.createRowBatchWithHiddenColumn(
                1, TypeDescription.VectorLayout.NONE);
        batch.cols[0].add(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
        batch.cols[1].add(timestamp);
        batch.size = 1;
        return batch.serialize();
    }

    private static long objectIdFromPath(String path)
    {
        return Long.parseLong(path.substring(path.lastIndexOf('_') + 1));
    }

    private static void sleepQuietly(long millis) throws IOException
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
