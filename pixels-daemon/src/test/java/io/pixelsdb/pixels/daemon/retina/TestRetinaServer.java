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
package io.pixelsdb.pixels.daemon.retina;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.ResolvedPrimary;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.LocalIndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.Permission;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.daemon.ServerContainer;
import io.pixelsdb.pixels.daemon.metadata.MetadataServer;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRetinaServer
{
    @Ignore("Integration test requires real metadata server, metadata DB, and fixed local ports.")
    @Test
    public void testRetinaServer()
    {
        ServerContainer container = new ServerContainer();
        MetadataServer metadataServer = new MetadataServer(18888);
        container.addServer("metadata server", metadataServer);
        RetinaServer retinaServer = new RetinaServer(18890);
        container.addServer("retina server", retinaServer);
    }

    @Test
    public void testRetinaServerImplInitializationFailureIsFailClosed() throws Exception
    {
        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(LocalIndexService.class);
        RetinaResourceManager resourceManager = mock(RetinaResourceManager.class);

        when(metadataService.getSchemas()).thenThrow(new MetadataException("metadata unavailable"));

        try
        {
            RetinaServerImpl server = new RetinaServerImpl(metadataService, indexService, resourceManager);
            fail("RetinaServerImpl must fail closed when initialization fails: " + server);
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("Failed to initialize RetinaServerImpl"));
        }

        verify(resourceManager).recoverOffloadCheckpoints();
        verify(resourceManager, never()).startBackgroundGc();
    }

    @Test
    public void testRetinaServerImplStartsBackgroundGcAfterSuccessfulInitialization() throws Exception
    {
        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(LocalIndexService.class);
        RetinaResourceManager resourceManager = mock(RetinaResourceManager.class);

        Schema schema = new Schema();
        schema.setName("gc_schema");
        Table table = new Table();
        table.setName("gc_table");
        Path orderedPath = new Path();
        orderedPath.setId(11L);
        orderedPath.setUri("file:///tmp/pixels/ordered");
        Path compactPath = new Path();
        compactPath.setId(12L);
        compactPath.setUri("file:///tmp/pixels/compact");
        Layout layout = new Layout();
        layout.setPermission(Permission.READ_WRITE);
        layout.setOrderedPaths(Collections.singletonList(orderedPath));
        layout.setCompactPaths(Collections.singletonList(compactPath));
        File orderedFile = new File();
        orderedFile.setName("ordered.pxl");
        File compactFile = new File();
        compactFile.setName("compact.pxl");
        List<String> lifecycleEvents = Collections.synchronizedList(new ArrayList<>());

        when(metadataService.getSchemas()).thenReturn(Collections.singletonList(schema));
        when(metadataService.getTables(schema.getName())).thenReturn(Collections.singletonList(table));
        when(metadataService.getLayouts(schema.getName(), table.getName())).thenReturn(Collections.singletonList(layout));
        when(metadataService.getRegularFiles(orderedPath.getId())).thenReturn(Collections.singletonList(orderedFile));
        when(metadataService.getRegularFiles(compactPath.getId())).thenReturn(Collections.singletonList(compactFile));
        doAnswer(invocation -> {
            lifecycleEvents.add("recover");
            return null;
        }).when(resourceManager).recoverOffloadCheckpoints();
        doAnswer(invocation -> {
            lifecycleEvents.add("visibility:" + invocation.getArgument(0));
            return null;
        }).when(resourceManager).addVisibility(org.mockito.ArgumentMatchers.anyString());
        doAnswer(invocation -> {
            lifecycleEvents.add("writeBuffer");
            return null;
        }).when(resourceManager).addWriteBuffer(schema.getName(), table.getName());
        doAnswer(invocation -> {
            lifecycleEvents.add("startGc");
            return null;
        }).when(resourceManager).startBackgroundGc();

        new RetinaServerImpl(metadataService, indexService, resourceManager);

        assertTrue(lifecycleEvents.indexOf("recover") >= 0);
        assertTrue(lifecycleEvents.contains("visibility:file:///tmp/pixels/ordered/ordered.pxl"));
        assertTrue(lifecycleEvents.contains("visibility:file:///tmp/pixels/compact/compact.pxl"));
        int writeBufferIndex = lifecycleEvents.indexOf("writeBuffer");
        assertTrue(writeBufferIndex > lifecycleEvents.indexOf("recover"));
        assertTrue(writeBufferIndex > lifecycleEvents.indexOf("visibility:file:///tmp/pixels/ordered/ordered.pxl"));
        assertTrue(writeBufferIndex > lifecycleEvents.indexOf("visibility:file:///tmp/pixels/compact/compact.pxl"));
        assertTrue(lifecycleEvents.indexOf("startGc") > writeBufferIndex);
        verify(resourceManager).addVisibility("file:///tmp/pixels/ordered/ordered.pxl");
        verify(resourceManager).addVisibility("file:///tmp/pixels/compact/compact.pxl");
        verify(resourceManager).startBackgroundGc();
    }

    @Test
    public void testRetinaServerImplBackgroundGcStartFailureIsFailClosed() throws Exception
    {
        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(LocalIndexService.class);
        RetinaResourceManager resourceManager = mock(RetinaResourceManager.class);

        when(metadataService.getSchemas()).thenReturn(Collections.emptyList());
        doThrow(new RetinaException("gc disabled by invalid lifecycle"))
                .when(resourceManager).startBackgroundGc();

        try
        {
            RetinaServerImpl server = new RetinaServerImpl(metadataService, indexService, resourceManager);
            fail("RetinaServerImpl must fail closed when background GC cannot start: " + server);
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("Failed to initialize RetinaServerImpl"));
        }

        InOrder inOrder = inOrder(resourceManager);
        inOrder.verify(resourceManager).recoverOffloadCheckpoints();
        inOrder.verify(resourceManager).startBackgroundGc();
    }

    // =====================================================================
    // UpdateRecord write paths.
    // =====================================================================

    /**
     * Build a RetinaServerImpl with the bare-minimum mocks needed to reach updateRecord
     * without performing real metadata work or any background initialisation.
     */
    private RetinaServerImpl buildServerWithLocalIndex(LocalIndexService localIndex,
                                                      RetinaResourceManager rm) throws Exception
    {
        MetadataService metadataService = mock(MetadataService.class);
        when(metadataService.getSchemas()).thenReturn(Collections.emptyList());
        return new RetinaServerImpl(metadataService, localIndex, rm);
    }

    private static IndexProto.IndexKey makeKey(long tableId, long indexId, String key, long ts)
    {
        return IndexProto.IndexKey.newBuilder()
                .setTableId(tableId).setIndexId(indexId)
                .setKey(ByteString.copyFromUtf8(key))
                .setTimestamp(ts)
                .build();
    }

    private static IndexProto.RowLocation makeLoc(long fileId, int rgId, int rgRowOffset)
    {
        return IndexProto.RowLocation.newBuilder()
                .setFileId(fileId).setRgId(rgId).setRgRowOffset(rgRowOffset)
                .build();
    }

    private static RetinaProto.UpdateRecordRequest makeDeleteRequest(long tableId, long indexId,
                                                                     String schema, String table,
                                                                     long ts, String... keys)
    {
        RetinaProto.TableUpdateData.Builder tud = RetinaProto.TableUpdateData.newBuilder()
                .setTableName(table)
                .setPrimaryIndexId(indexId)
                .setTimestamp(ts);
        for (String k : keys)
        {
            tud.addDeleteData(RetinaProto.DeleteData.newBuilder()
                    .addIndexKeys(makeKey(tableId, indexId, k, ts)));
        }
        return RetinaProto.UpdateRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken("t"))
                .setSchemaName(schema)
                .addTableUpdateData(tud)
                .build();
    }

    private static RetinaProto.UpdateRecordRequest makeInsertRequest(long tableId, long indexId,
                                                                     String schema, String table,
                                                                     long ts, String... keys)
    {
        RetinaProto.TableUpdateData.Builder tud = RetinaProto.TableUpdateData.newBuilder()
                .setTableName(table)
                .setPrimaryIndexId(indexId)
                .setTimestamp(ts);
        for (String k : keys)
        {
            tud.addInsertData(RetinaProto.InsertData.newBuilder()
                    .addIndexKeys(makeKey(tableId, indexId, k, ts))
                    .addColValues(ByteString.copyFromUtf8("v-" + k)));
        }
        return RetinaProto.UpdateRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken("t"))
                .setSchemaName(schema)
                .addTableUpdateData(tud)
                .build();
    }

    private static RetinaProto.UpdateRecordRequest makeDeleteWithSecondaryRequest(
            long tableId, long primaryIndexId, long secondaryIndexId,
            String schema, String table, long ts, String key)
    {
        RetinaProto.TableUpdateData.Builder tud = RetinaProto.TableUpdateData.newBuilder()
                .setTableName(table)
                .setPrimaryIndexId(primaryIndexId)
                .setTimestamp(ts)
                .addDeleteData(RetinaProto.DeleteData.newBuilder()
                        .addIndexKeys(makeKey(tableId, primaryIndexId, key, ts))
                        .addIndexKeys(makeKey(tableId, secondaryIndexId, "sec-" + key, ts)));
        return RetinaProto.UpdateRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken("t"))
                .setSchemaName(schema)
                .addTableUpdateData(tud)
                .build();
    }

    private static IndexProto.PrimaryIndexEntry.Builder makePrimaryEntryBuilder(
            IndexProto.IndexKey key, long rowId, IndexProto.RowLocation location)
    {
        return IndexProto.PrimaryIndexEntry.newBuilder()
                .setIndexKey(key)
                .setRowId(rowId)
                .setRowLocation(location);
    }

    @Test
    public void testStagedDeleteCallsResolveBeforeDeleteRecordThenTombstone() throws Exception
    {
        LocalIndexService localIndex = mock(LocalIndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);

        long tableId = 1L;
        long indexId = 100L;
        long ts = 12345L;
        IndexProto.IndexKey foundKey = makeKey(tableId, indexId, "k-found", ts);
        IndexProto.IndexKey missKey = makeKey(tableId, indexId, "k-miss", ts);
        IndexProto.RowLocation foundLoc = makeLoc(7L, 0, 3);

        when(localIndex.resolvePrimary(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any()))
                .thenReturn(Arrays.asList(
                        Optional.of(new ResolvedPrimary(42L, foundLoc)),
                        Optional.empty()));

        RetinaServerImpl server = buildServerWithLocalIndex(localIndex, rm);

        AtomicReference<RetinaProto.UpdateRecordResponse> respHolder = new AtomicReference<>();
        StreamObserver<RetinaProto.UpdateRecordResponse> observer = new StreamObserver<RetinaProto.UpdateRecordResponse>()
        {
            @Override public void onNext(RetinaProto.UpdateRecordResponse v) { respHolder.set(v); }
            @Override public void onError(Throwable t) { fail(t.getMessage()); }
            @Override public void onCompleted() { }
        };

        server.updateRecord(makeDeleteRequest(tableId, indexId, "s", "tbl", ts, "k-found", "k-miss"), observer);
        assertNotNull(respHolder.get());
        assertEquals(0, respHolder.get().getHeader().getErrorCode());

        InOrder inOrder = inOrder(localIndex, rm);
        inOrder.verify(localIndex).resolvePrimary(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any());
        // Only the FOUND key triggers deleteRecord and contributes to the tombstone list.
        inOrder.verify(rm).deleteRecord(eq(foundLoc), eq(ts));
        inOrder.verify(localIndex).deletePrimaryIndexEntriesOnly(eq(tableId), eq(indexId),
                eq(Collections.singletonList(foundKey)), any());

        verify(localIndex, never()).deletePrimaryIndexEntries(anyLong(), anyLong(),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any());
    }

    @Test
    public void testStagedDeleteAllNotFoundProducesNoTombstone() throws Exception
    {
        LocalIndexService localIndex = mock(LocalIndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);

        long tableId = 1L;
        long indexId = 100L;
        long ts = 1L;
        when(localIndex.resolvePrimary(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any()))
                .thenReturn(Collections.singletonList(Optional.<ResolvedPrimary>empty()));

        RetinaServerImpl server = buildServerWithLocalIndex(localIndex, rm);

        AtomicReference<RetinaProto.UpdateRecordResponse> respHolder = new AtomicReference<>();
        server.updateRecord(makeDeleteRequest(tableId, indexId, "s", "tbl", ts, "absent"),
                new StreamObserver<RetinaProto.UpdateRecordResponse>()
                {
                    @Override public void onNext(RetinaProto.UpdateRecordResponse v) { respHolder.set(v); }
                    @Override public void onError(Throwable t) { fail(t.getMessage()); }
                    @Override public void onCompleted() { }
                });

        assertNotNull(respHolder.get());
        assertEquals(0, respHolder.get().getHeader().getErrorCode());
        verify(rm, never()).deleteRecord(any(IndexProto.RowLocation.class), anyLong());
        verify(localIndex, never()).deletePrimaryIndexEntriesOnly(anyLong(), anyLong(),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any());
    }

    @Test
    public void testStagedDeleteSecondaryFailureIsBestEffort() throws Exception
    {
        LocalIndexService localIndex = mock(LocalIndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);

        long tableId = 1L;
        long primaryIndexId = 100L;
        long secondaryIndexId = 200L;
        long ts = 9L;
        IndexProto.RowLocation loc = makeLoc(7L, 0, 3);
        when(localIndex.resolvePrimary(eq(tableId), eq(primaryIndexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any()))
                .thenReturn(Collections.singletonList(Optional.of(new ResolvedPrimary(42L, loc))));
        doThrow(new IndexException("secondary already tombstoned"))
                .when(localIndex).deleteSecondaryIndexEntries(eq(tableId), eq(secondaryIndexId),
                        ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any());

        RetinaServerImpl server = buildServerWithLocalIndex(localIndex, rm);
        AtomicReference<RetinaProto.UpdateRecordResponse> respHolder = new AtomicReference<>();
        server.updateRecord(makeDeleteWithSecondaryRequest(tableId, primaryIndexId, secondaryIndexId,
                        "s", "tbl", ts, "k"),
                new StreamObserver<RetinaProto.UpdateRecordResponse>()
                {
                    @Override public void onNext(RetinaProto.UpdateRecordResponse v) { respHolder.set(v); }
                    @Override public void onError(Throwable t) { fail(t.getMessage()); }
                    @Override public void onCompleted() { }
                });

        assertNotNull(respHolder.get());
        assertEquals(0, respHolder.get().getHeader().getErrorCode());
        verify(rm).deleteRecord(eq(loc), eq(ts));
        verify(localIndex).deletePrimaryIndexEntriesOnly(eq(tableId), eq(primaryIndexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any());
    }

    @Test
    public void testStagedInsertWritesMainBeforePrimary() throws Exception
    {
        LocalIndexService localIndex = mock(LocalIndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);

        long tableId = 1L;
        long indexId = 100L;
        long ts = 123L;
        IndexProto.IndexKey key = makeKey(tableId, indexId, "k-insert", ts);
        IndexProto.RowLocation loc = makeLoc(70L, 0, 4);
        when(rm.insertRecord(eq("s"), eq("tbl"), ArgumentMatchers.<byte[][]>any(), eq(ts), eq(0)))
                .thenReturn(makePrimaryEntryBuilder(key, 51L, loc));

        RetinaServerImpl server = buildServerWithLocalIndex(localIndex, rm);
        AtomicReference<RetinaProto.UpdateRecordResponse> respHolder = new AtomicReference<>();
        server.updateRecord(makeInsertRequest(tableId, indexId, "s", "tbl", ts, "k-insert"),
                new StreamObserver<RetinaProto.UpdateRecordResponse>()
                {
                    @Override public void onNext(RetinaProto.UpdateRecordResponse v) { respHolder.set(v); }
                    @Override public void onError(Throwable t) { fail(t.getMessage()); }
                    @Override public void onCompleted() { }
                });

        assertNotNull(respHolder.get());
        assertEquals(0, respHolder.get().getHeader().getErrorCode());
        InOrder inOrder = inOrder(localIndex);
        inOrder.verify(localIndex).putMainIndexEntriesOnly(eq(tableId),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any());
        inOrder.verify(localIndex).putPrimaryIndexEntriesOnly(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());
    }

    @Test
    public void testStagedInsertPrimaryFailureMasksInsertedRows() throws Exception
    {
        LocalIndexService localIndex = mock(LocalIndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);

        long tableId = 1L;
        long indexId = 100L;
        long ts = 124L;
        IndexProto.IndexKey key0 = makeKey(tableId, indexId, "k0", ts);
        IndexProto.IndexKey key1 = makeKey(tableId, indexId, "k1", ts);
        IndexProto.RowLocation loc0 = makeLoc(71L, 0, 0);
        IndexProto.RowLocation loc1 = makeLoc(71L, 0, 1);
        when(rm.insertRecord(eq("s"), eq("tbl"), ArgumentMatchers.<byte[][]>any(), eq(ts), eq(0)))
                .thenReturn(makePrimaryEntryBuilder(key0, 61L, loc0),
                        makePrimaryEntryBuilder(key1, 62L, loc1));
        doThrow(new IndexException("primary write failed"))
                .when(localIndex).putPrimaryIndexEntriesOnly(eq(tableId), eq(indexId),
                        ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());

        RetinaServerImpl server = buildServerWithLocalIndex(localIndex, rm);
        AtomicReference<RetinaProto.UpdateRecordResponse> respHolder = new AtomicReference<>();
        server.updateRecord(makeInsertRequest(tableId, indexId, "s", "tbl", ts, "k0", "k1"),
                new StreamObserver<RetinaProto.UpdateRecordResponse>()
                {
                    @Override public void onNext(RetinaProto.UpdateRecordResponse v) { respHolder.set(v); }
                    @Override public void onError(Throwable t) { fail(t.getMessage()); }
                    @Override public void onCompleted() { }
                });

        assertNotNull(respHolder.get());
        assertEquals(2, respHolder.get().getHeader().getErrorCode());
        verify(rm).deleteRecord(eq(loc0), eq(ts));
        verify(rm).deleteRecord(eq(loc1), eq(ts));
    }

    @Test
    public void testUpdateDataUsesStagedUpdateIndexPath() throws Exception
    {
        LocalIndexService localIndex = mock(LocalIndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);

        long tableId = 1L;
        long indexId = 100L;
        long secondaryIndexId = 200L;
        long ts = 7L;
        IndexProto.IndexKey key = makeKey(tableId, indexId, "k-upd", ts);
        IndexProto.IndexKey secondaryKey = makeKey(tableId, secondaryIndexId, "sec-k-upd", ts);
        IndexProto.RowLocation prevLoc = makeLoc(7L, 0, 3);
        IndexProto.RowLocation newLoc = makeLoc(70L, 0, 4);

        when(localIndex.resolvePrimary(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any()))
                .thenReturn(Collections.singletonList(Optional.of(new ResolvedPrimary(42L, prevLoc))));
        when(rm.insertRecord(eq("s"), eq("tbl"), ArgumentMatchers.<byte[][]>any(), eq(ts), eq(0)))
                .thenReturn(makePrimaryEntryBuilder(key, 99L, newLoc));

        RetinaProto.UpdateRecordRequest req = RetinaProto.UpdateRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken("t"))
                .setSchemaName("s")
                .addTableUpdateData(RetinaProto.TableUpdateData.newBuilder()
                        .setTableName("tbl")
                        .setPrimaryIndexId(indexId)
                        .setTimestamp(ts)
                        .addUpdateData(RetinaProto.UpdateData.newBuilder()
                                .addIndexKeys(key)
                                .addIndexKeys(secondaryKey)
                                .addColValues(ByteString.copyFromUtf8("v"))))
                .build();

        RetinaServerImpl server = buildServerWithLocalIndex(localIndex, rm);
        AtomicReference<RetinaProto.UpdateRecordResponse> respHolder = new AtomicReference<>();
        server.updateRecord(req, new StreamObserver<RetinaProto.UpdateRecordResponse>()
        {
            @Override public void onNext(RetinaProto.UpdateRecordResponse v) { respHolder.set(v); }
            @Override public void onError(Throwable t) { fail(t.getMessage()); }
            @Override public void onCompleted() { }
        });

        assertNotNull(respHolder.get());
        assertEquals(0, respHolder.get().getHeader().getErrorCode());

        InOrder inOrder = inOrder(localIndex, rm);
        inOrder.verify(localIndex).resolvePrimary(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any());
        inOrder.verify(rm).insertRecord(eq("s"), eq("tbl"), ArgumentMatchers.<byte[][]>any(),
                eq(ts), eq(0));
        inOrder.verify(localIndex).putMainIndexEntriesOnly(eq(tableId),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any());
        inOrder.verify(localIndex).updatePrimaryIndexEntriesOnly(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());
        inOrder.verify(rm).deleteRecord(eq(prevLoc), eq(ts));
        inOrder.verify(localIndex).updateSecondaryIndexEntries(eq(tableId), eq(secondaryIndexId),
                ArgumentMatchers.<List<IndexProto.SecondaryIndexEntry>>any(), any());

        verify(localIndex, never()).deletePrimaryIndexEntriesOnly(anyLong(), anyLong(),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any());
        verify(localIndex, never()).putPrimaryIndexEntriesOnly(anyLong(), anyLong(),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());
        verify(localIndex, never()).updatePrimaryIndexEntries(anyLong(), anyLong(),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());
    }

    @Test
    public void testStagedUpdatePrimaryFailureMasksInsertedRows() throws Exception
    {
        LocalIndexService localIndex = mock(LocalIndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);

        long tableId = 1L;
        long indexId = 100L;
        long ts = 8L;
        IndexProto.IndexKey key = makeKey(tableId, indexId, "k-upd-fail", ts);
        IndexProto.RowLocation prevLoc = makeLoc(7L, 0, 3);
        IndexProto.RowLocation newLoc = makeLoc(70L, 0, 4);

        when(localIndex.resolvePrimary(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any()))
                .thenReturn(Collections.singletonList(Optional.of(new ResolvedPrimary(42L, prevLoc))));
        when(rm.insertRecord(eq("s"), eq("tbl"), ArgumentMatchers.<byte[][]>any(), eq(ts), eq(0)))
                .thenReturn(makePrimaryEntryBuilder(key, 99L, newLoc));
        doThrow(new IndexException("primary update failed"))
                .when(localIndex).updatePrimaryIndexEntriesOnly(eq(tableId), eq(indexId),
                        ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());

        RetinaProto.UpdateRecordRequest req = RetinaProto.UpdateRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken("t"))
                .setSchemaName("s")
                .addTableUpdateData(RetinaProto.TableUpdateData.newBuilder()
                        .setTableName("tbl")
                        .setPrimaryIndexId(indexId)
                        .setTimestamp(ts)
                        .addUpdateData(RetinaProto.UpdateData.newBuilder()
                                .addIndexKeys(key)
                                .addColValues(ByteString.copyFromUtf8("v"))))
                .build();

        RetinaServerImpl server = buildServerWithLocalIndex(localIndex, rm);
        AtomicReference<RetinaProto.UpdateRecordResponse> respHolder = new AtomicReference<>();
        server.updateRecord(req, new StreamObserver<RetinaProto.UpdateRecordResponse>()
        {
            @Override public void onNext(RetinaProto.UpdateRecordResponse v) { respHolder.set(v); }
            @Override public void onError(Throwable t) { fail(t.getMessage()); }
            @Override public void onCompleted() { }
        });

        assertNotNull(respHolder.get());
        assertEquals(2, respHolder.get().getHeader().getErrorCode());

        InOrder inOrder = inOrder(localIndex, rm);
        inOrder.verify(localIndex).resolvePrimary(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any());
        inOrder.verify(rm).insertRecord(eq("s"), eq("tbl"), ArgumentMatchers.<byte[][]>any(),
                eq(ts), eq(0));
        inOrder.verify(localIndex).putMainIndexEntriesOnly(eq(tableId),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any());
        inOrder.verify(localIndex).updatePrimaryIndexEntriesOnly(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());
        inOrder.verify(rm).deleteRecord(eq(newLoc), eq(ts));
        verify(rm, never()).deleteRecord(eq(prevLoc), eq(ts));
        verify(localIndex, never()).putPrimaryIndexEntriesOnly(anyLong(), anyLong(),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());
    }

    @Test
    public void testStagedUpdateMissingPrimaryFailsBeforeAppend() throws Exception
    {
        LocalIndexService localIndex = mock(LocalIndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);

        long tableId = 1L;
        long indexId = 100L;
        long ts = 9L;
        IndexProto.IndexKey key = makeKey(tableId, indexId, "k-upd-missing", ts);

        when(localIndex.resolvePrimary(eq(tableId), eq(indexId),
                ArgumentMatchers.<List<IndexProto.IndexKey>>any(), any()))
                .thenReturn(Collections.singletonList(Optional.<ResolvedPrimary>empty()));

        RetinaProto.UpdateRecordRequest req = RetinaProto.UpdateRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder().setToken("t"))
                .setSchemaName("s")
                .addTableUpdateData(RetinaProto.TableUpdateData.newBuilder()
                        .setTableName("tbl")
                        .setPrimaryIndexId(indexId)
                        .setTimestamp(ts)
                        .addUpdateData(RetinaProto.UpdateData.newBuilder()
                                .addIndexKeys(key)
                                .addColValues(ByteString.copyFromUtf8("v"))))
                .build();

        RetinaServerImpl server = buildServerWithLocalIndex(localIndex, rm);
        AtomicReference<RetinaProto.UpdateRecordResponse> respHolder = new AtomicReference<>();
        server.updateRecord(req, new StreamObserver<RetinaProto.UpdateRecordResponse>()
        {
            @Override public void onNext(RetinaProto.UpdateRecordResponse v) { respHolder.set(v); }
            @Override public void onError(Throwable t) { fail(t.getMessage()); }
            @Override public void onCompleted() { }
        });

        assertNotNull(respHolder.get());
        assertEquals(2, respHolder.get().getHeader().getErrorCode());
        verify(rm, never()).insertRecord(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.<byte[][]>any(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt());
        verify(localIndex, never()).putMainIndexEntriesOnly(anyLong(),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any());
        verify(localIndex, never()).updatePrimaryIndexEntriesOnly(anyLong(), anyLong(),
                ArgumentMatchers.<List<IndexProto.PrimaryIndexEntry>>any(), any());
    }

    @Test
    public void testFailsClosedOnNonLocalIndexService() throws Exception
    {
        // UpdateRecord uses LocalIndexService-only primary-index operations.
        IndexService nonLocal = mock(IndexService.class);
        RetinaResourceManager rm = mock(RetinaResourceManager.class);
        MetadataService md = mock(MetadataService.class);
        try
        {
            new RetinaServerImpl(md, nonLocal, rm);
            fail("RetinaServerImpl must require LocalIndexService");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("LocalIndexService")
                    || (e.getCause() != null && e.getCause().getMessage() != null
                        && e.getCause().getMessage().contains("LocalIndexService")));
        }
    }
}
