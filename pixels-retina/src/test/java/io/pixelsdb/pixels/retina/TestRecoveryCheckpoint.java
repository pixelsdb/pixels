/*
 * Copyright 2026 PixelsDB.
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

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RecoveryCheckpoint}: the {@link RecoveryCheckpoint.Body}
 * serialization codec, {@link RecoveryCheckpoint#generate(long, List, List)}'s
 * write/publish contract, and {@link RecoveryCheckpoint#load()}'s fail-closed
 * contract.
 * <p>
 * The load() contract under test: only an absent pointer is a legitimate
 * fresh-deployment signal ({@code null}); once the pointer exists, any unusable
 * body — transient read error, empty/corrupted/truncated body, stale vnode
 * mapping, mismatched node id, illegal timestamp — aborts recovery by throwing,
 * never a silent fresh deployment.
 */
public class TestRecoveryCheckpoint
{
    private static final String NODE_ID = "host1:8080";
    private static final int VNODES = 4;
    private static final String BODY_PATH = "checkpoint/recovery/host1/body-1";
    private static final String POINTER_KEY = "/pixels/retina/recovery/checkpoint/" + NODE_ID + "/current";

    private Storage storage;
    private EtcdUtil etcd;
    private RecoveryCheckpoint checkpoint;
    private List<String> createdPaths;
    private List<ByteArrayOutputStream> createdBodies;

    @Before
    public void setUp()
    {
        storage = mock(Storage.class);
        etcd = mock(EtcdUtil.class);
        checkpoint = new RecoveryCheckpoint(storage, "checkpoint/recovery", etcd, VNODES, NODE_ID);
        createdPaths = new ArrayList<>();
        createdBodies = new ArrayList<>();
    }

    // ===============================================================
    //   Shared helpers
    // ===============================================================

    private static RecoveryCheckpoint.Body.Builder baseBuilder()
    {
        return RecoveryCheckpoint.Body.builder()
                .retinaNodeId(NODE_ID)
                .writeTimeMs(1000L)
                .checkpointAppliedTs(3000L)
                .virtualNodesPerNode(VNODES);
    }

    private static byte[] serialize(RecoveryCheckpoint.Body body) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        body.writeTo(new DataOutputStream(baos));
        return baos.toByteArray();
    }

    private static String generatedBodyPath(long checkpointAppliedTs)
    {
        return "checkpoint/recovery/recovery_" + NODE_ID + "_" + checkpointAppliedTs + ".bin";
    }

    /** Assert every field of two Body objects is identical. */
    private static void assertBodyEquals(RecoveryCheckpoint.Body expected, RecoveryCheckpoint.Body actual)
    {
        assertEquals(expected.getRetinaNodeId(), actual.getRetinaNodeId());
        assertEquals(expected.getWriteTimeMs(), actual.getWriteTimeMs());
        assertEquals(expected.getCheckpointAppliedTs(), actual.getCheckpointAppliedTs());
        assertEquals(expected.getVirtualNodesPerNode(), actual.getVirtualNodesPerNode());

        List<RecoveryCheckpoint.PendingSegmentEntry> es = expected.getSegmentEntries();
        List<RecoveryCheckpoint.PendingSegmentEntry> as = actual.getSegmentEntries();
        assertEquals(es.size(), as.size());
        for (int i = 0; i < es.size(); i++)
        {
            assertEquals("segment[" + i + "].virtualNodeId", es.get(i).getVirtualNodeId(), as.get(i).getVirtualNodeId());
            assertEquals("segment[" + i + "].minCommitTs", es.get(i).getMinCommitTs(), as.get(i).getMinCommitTs());
        }

        List<RecoveryCheckpoint.VisibilityEntry> er = expected.getRgEntries();
        List<RecoveryCheckpoint.VisibilityEntry> ar = actual.getRgEntries();
        assertEquals(er.size(), ar.size());
        for (int i = 0; i < er.size(); i++)
        {
            assertEquals("rg[" + i + "].fileId", er.get(i).getFileId(), ar.get(i).getFileId());
            assertEquals("rg[" + i + "].rgId", er.get(i).getRgId(), ar.get(i).getRgId());
            assertEquals("rg[" + i + "].recordNum", er.get(i).getRecordNum(), ar.get(i).getRecordNum());
            assertEquals("rg[" + i + "].baseTimestamp", er.get(i).getBaseTimestamp(), ar.get(i).getBaseTimestamp());
            assertArrayEquals("rg[" + i + "].bitmap", er.get(i).getBitmap(), ar.get(i).getBitmap());
        }
    }

    /** Serialize → deserialize → re-serialize: both byte arrays must be identical. */
    private static void assertSerializeStable(RecoveryCheckpoint.Body body) throws IOException, RetinaException
    {
        byte[] first = serialize(body);
        byte[] second = serialize(RecoveryCheckpoint.Body.readFrom(first));
        assertArrayEquals("re-serialized bytes differ", first, second);
    }

    /** Stub the etcd pointer to reference {@code value} (null/empty ⇒ absent). */
    private void stubPointer(String value)
    {
        if (value == null)
        {
            when(etcd.getKeyValue(anyString())).thenReturn(null);
            return;
        }
        KeyValue kv = mock(KeyValue.class);
        when(kv.getValue()).thenReturn(ByteSequence.from(value, StandardCharsets.UTF_8));
        when(etcd.getKeyValue(anyString())).thenReturn(kv);
    }

    private void stubBodyBytes(byte[] bytes) throws IOException
    {
        when(storage.open(anyString())).thenReturn(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    private void stubCreateCapturingBody() throws IOException
    {
        when(storage.create(anyString(), eq(true), anyInt())).thenAnswer(captureCreate());
    }

    private Answer<DataOutputStream> captureCreate()
    {
        return invocation -> {
            createdPaths.add(invocation.getArgument(0));
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            createdBodies.add(bodyBytes);
            return new DataOutputStream(bodyBytes);
        };
    }

    private static void putInt(byte[] bytes, int offset, int value)
    {
        ByteBuffer.wrap(bytes).putInt(offset, value);
    }

    private static void assertSegmentEntry(RecoveryCheckpoint.PendingSegmentEntry entry,
                                           int virtualNodeId,
                                           long minCommitTs)
    {
        assertEquals(virtualNodeId, entry.getVirtualNodeId());
        assertEquals(minCommitTs, entry.getMinCommitTs());
    }

    private static void assertRgEntry(RecoveryCheckpoint.VisibilityEntry entry,
                                      long fileId,
                                      int rgId,
                                      int recordNum,
                                      long baseTimestamp,
                                      long[] bitmap)
    {
        assertEquals(fileId, entry.getFileId());
        assertEquals(rgId, entry.getRgId());
        assertEquals(recordNum, entry.getRecordNum());
        assertEquals(baseTimestamp, entry.getBaseTimestamp());
        assertArrayEquals(bitmap, entry.getBitmap());
    }

    // ===============================================================
    //   Body codec — roundtrip
    // ===============================================================

    @Test
    public void testRoundtrip_emptyBody() throws IOException, RetinaException
    {
        RecoveryCheckpoint.Body original = baseBuilder().build();
        assertBodyEquals(original, RecoveryCheckpoint.Body.readFrom(serialize(original)));
        assertSerializeStable(original);
    }

    @Test
    public void testRoundtrip_full() throws IOException, RetinaException
    {
        RecoveryCheckpoint.Body original = baseBuilder()
                .segmentEntries(Arrays.asList(
                        new RecoveryCheckpoint.PendingSegmentEntry(1, 500L),
                        new RecoveryCheckpoint.PendingSegmentEntry(2, 600L)))
                .rgEntries(Arrays.asList(
                        new RecoveryCheckpoint.VisibilityEntry(1L, 0, 100, 1000L, new long[]{0xFFL}),
                        new RecoveryCheckpoint.VisibilityEntry(2L, 1, 200, 2000L, new long[]{0xAAL, 0xBBL}),
                        new RecoveryCheckpoint.VisibilityEntry(3L, 2, 300, 3000L, new long[]{})))
                .build();
        assertBodyEquals(original, RecoveryCheckpoint.Body.readFrom(serialize(original)));
        assertSerializeStable(original);
    }

    @Test
    public void testRoundtrip_nullBitmap_treatedAsEmpty() throws IOException, RetinaException
    {
        // null bitmap is normalised to empty array on write; verify the parsed result reflects that
        RecoveryCheckpoint.Body original = baseBuilder().rgEntries(Collections.singletonList(
                new RecoveryCheckpoint.VisibilityEntry(1L, 0, 0, 1000L, null)
        )).build();
        RecoveryCheckpoint.Body parsed = RecoveryCheckpoint.Body.readFrom(serialize(original));
        assertArrayEquals(new long[0], parsed.getRgEntries().get(0).getBitmap());
        assertSerializeStable(parsed);
    }

    /** Multi-word bitmaps and a multibyte (non-ASCII) nodeId must survive a roundtrip. */
    @Test
    public void testRoundtrip_multiWordBitmapAndUnicodeNodeId() throws IOException, RetinaException
    {
        RecoveryCheckpoint.Body original = RecoveryCheckpoint.Body.builder()
                .retinaNodeId("节点-host①:8080")
                .writeTimeMs(1L)
                .checkpointAppliedTs(2L)
                .virtualNodesPerNode(7)
                .rgEntries(Collections.singletonList(new RecoveryCheckpoint.VisibilityEntry(
                        99L, 5, 4096, 123456789L,
                        new long[]{-1L, 0L, 0x1234_5678_9ABC_DEF0L, Long.MIN_VALUE, Long.MAX_VALUE})))
                .build();
        assertBodyEquals(original, RecoveryCheckpoint.Body.readFrom(serialize(original)));
    }

    /** Many entries exercise the count fields and ordering of the codec. */
    @Test
    public void testRoundtrip_manyEntries() throws IOException, RetinaException
    {
        List<RecoveryCheckpoint.PendingSegmentEntry> segs = new ArrayList<>();
        List<RecoveryCheckpoint.VisibilityEntry> rgs = new ArrayList<>();
        for (int i = 0; i < 256; i++)
        {
            segs.add(new RecoveryCheckpoint.PendingSegmentEntry(i % 8, 1000L + i));
            rgs.add(new RecoveryCheckpoint.VisibilityEntry(i, i % 4, i, 2000L + i, new long[]{(long) i}));
        }
        RecoveryCheckpoint.Body original = baseBuilder().segmentEntries(segs).rgEntries(rgs).build();
        assertBodyEquals(original, RecoveryCheckpoint.Body.readFrom(serialize(original)));
    }

    // ===============================================================
    //   Body codec — corruption is rejected
    // ===============================================================

    @Test
    public void testReadFrom_emptyBytes_throws()
    {
        assertThrows(RetinaException.class, () -> RecoveryCheckpoint.Body.readFrom(new byte[0]));
        assertThrows(RetinaException.class, () -> RecoveryCheckpoint.Body.readFrom(null));
    }

    @Test
    public void testReadFrom_badMagic_throws()
    {
        assertThrows(RetinaException.class, () -> RecoveryCheckpoint.Body.readFrom(new byte[64]));
    }

    /**
     * A truncated body (e.g. a half-written body from a power loss) must be
     * rejected, not silently parsed. This underpins load()'s fail-closed
     * contract: corruption is never mistaken for "no checkpoint".
     */
    @Test
    public void testReadFrom_truncatedBody_throws() throws IOException
    {
        byte[] full = serialize(baseBuilder()
                .rgEntries(Collections.singletonList(
                        new RecoveryCheckpoint.VisibilityEntry(1L, 0, 100, 1000L, new long[]{0xFFL, 0xEEL})))
                .build());
        // Drop the trailing bytes so the final bitmap long is incomplete.
        for (int cut = 1; cut <= 8; cut++)
        {
            byte[] truncated = Arrays.copyOf(full, full.length - cut);
            assertThrows("truncation of " + cut + " byte(s) must be rejected",
                    RetinaException.class, () -> RecoveryCheckpoint.Body.readFrom(truncated));
        }
    }

    /** Trailing bytes after a valid payload are corruption and must be rejected. */
    @Test
    public void testReadFrom_trailingBytes_throws() throws IOException
    {
        byte[] full = serialize(baseBuilder().build());
        byte[] padded = Arrays.copyOf(full, full.length + 4); // 4 trailing zero bytes
        assertThrows(RetinaException.class, () -> RecoveryCheckpoint.Body.readFrom(padded));
    }

    /** A header that declares a wildly large nodeId length must not allocate; it must throw. */
    @Test
    public void testReadFrom_oversizedNodeIdLen_throws() throws IOException
    {
        byte[] full = serialize(baseBuilder().build());
        // Layout: magic(4) writeTimeMs(8) checkpointAppliedTs(8) vnodes(4) segCount(4) rgCount(4) then nodeIdLen(4).
        byte[] corrupt = full.clone();
        int nodeIdLenOffset = 4 + 8 + 8 + 4 + 4 + 4;
        putInt(corrupt, nodeIdLenOffset, Integer.MAX_VALUE);
        assertThrows(RetinaException.class, () -> RecoveryCheckpoint.Body.readFrom(corrupt));
    }

    @Test
    public void testReadFrom_negativeEntryCount_throws() throws IOException
    {
        byte[] full = serialize(baseBuilder().build());
        byte[] corrupt = full.clone();
        int segmentCountOffset = 4 + 8 + 8 + 4;
        putInt(corrupt, segmentCountOffset, -1);
        assertThrows(RetinaException.class, () -> RecoveryCheckpoint.Body.readFrom(corrupt));
    }

    // ===============================================================
    //   Body builder — edge cases
    // ===============================================================

    @Test
    public void testBuilder_missingOrEmptyNodeId_throws()
    {
        assertThrows(IllegalArgumentException.class, () ->
                RecoveryCheckpoint.Body.builder()
                        .writeTimeMs(1000L)
                        .checkpointAppliedTs(3000L)
                        .virtualNodesPerNode(4)
                        .build());
        assertThrows(IllegalArgumentException.class, () ->
                RecoveryCheckpoint.Body.builder()
                        .retinaNodeId("")
                        .build());
    }

    @Test
    public void testBuilder_defaultsToEmptyLists()
    {
        RecoveryCheckpoint.Body body = baseBuilder().build();
        assertTrue(body.getSegmentEntries().isEmpty());
        assertTrue(body.getRgEntries().isEmpty());
    }

    /** Entry lists are defensively copied: mutating the source after build must not affect the body. */
    @Test
    public void testBuilder_defensiveCopy()
    {
        List<RecoveryCheckpoint.VisibilityEntry> source = new ArrayList<>();
        source.add(new RecoveryCheckpoint.VisibilityEntry(1L, 0, 0, 0L, new long[0]));
        RecoveryCheckpoint.Body body = baseBuilder().rgEntries(source).build();
        source.add(new RecoveryCheckpoint.VisibilityEntry(2L, 0, 0, 0L, new long[0]));
        assertEquals(1, body.getRgEntries().size());
    }

    /** Exposed entry lists are immutable. */
    @Test
    public void testBody_listsAreUnmodifiable()
    {
        RecoveryCheckpoint.Body body = baseBuilder().build();
        assertThrows(UnsupportedOperationException.class, () ->
                body.getRgEntries().add(new RecoveryCheckpoint.VisibilityEntry(1L, 0, 0, 0L, new long[0])));
        assertThrows(UnsupportedOperationException.class, () ->
                body.getSegmentEntries().add(new RecoveryCheckpoint.PendingSegmentEntry(0, 0L)));
    }

    // ===============================================================
    //   load() — only an absent pointer is a fresh-deployment signal
    // ===============================================================

    @Test
    public void testLoad_pointerAbsentOrEmpty_returnsNull() throws RetinaException
    {
        stubPointer(null);
        assertNull(checkpoint.load());

        stubPointer(""); // empty value is treated as absent by readPointer
        assertNull(checkpoint.load());
    }

    // ===============================================================
    //   load() — happy path
    // ===============================================================

    @Test
    public void testLoad_validBody_returnsLoaded() throws IOException, RetinaException
    {
        stubPointer(BODY_PATH);
        stubBodyBytes(serialize(baseBuilder()
                .rgEntries(Collections.singletonList(
                        new RecoveryCheckpoint.VisibilityEntry(7L, 0, 10, 2500L, new long[]{0x1L})))
                .build()));

        RecoveryCheckpoint.LoadedCheckpoint loaded = checkpoint.load();
        assertNotNull(loaded);
        assertEquals(BODY_PATH, loaded.bodyPath);
        assertEquals(NODE_ID, loaded.body.getRetinaNodeId());
        assertEquals(3000L, loaded.body.getCheckpointAppliedTs());
        assertEquals(1, loaded.body.getRgEntries().size());
    }

    // ===============================================================
    //   load() — fail-closed: pointer exists but body is unusable
    // ===============================================================

    /** Transient read failure must abort recovery, not fresh-deploy. */
    @Test
    public void testLoad_readThrows_failsClosed() throws IOException
    {
        stubPointer(BODY_PATH);
        when(storage.open(anyString())).thenThrow(new IOException("transient S3 read error"));
        assertThrows(RetinaException.class, () -> checkpoint.load());
    }

    /** An empty body file is treated as a read failure, not "no checkpoint". */
    @Test
    public void testLoad_emptyBodyFile_failsClosed() throws IOException
    {
        stubPointer(BODY_PATH);
        stubBodyBytes(new byte[0]);
        assertThrows(RetinaException.class, () -> checkpoint.load());
    }

    /** A corrupted/half-written body must abort recovery. */
    @Test
    public void testLoad_corruptBody_failsClosed() throws IOException
    {
        stubPointer(BODY_PATH);
        stubBodyBytes(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}); // bad magic / truncated
        assertThrows(RetinaException.class, () -> checkpoint.load());
    }

    /** A body written under a different node.virtual.num must abort recovery. */
    @Test
    public void testLoad_vnodeMismatch_failsClosed() throws IOException
    {
        stubPointer(BODY_PATH);
        stubBodyBytes(serialize(baseBuilder().virtualNodesPerNode(VNODES + 1).build()));
        assertThrows(RetinaException.class, () -> checkpoint.load());
    }

    /** A body belonging to a different retina node must abort recovery. */
    @Test
    public void testLoad_nodeIdMismatch_failsClosed() throws IOException
    {
        stubPointer(BODY_PATH);
        stubBodyBytes(serialize(baseBuilder().retinaNodeId("other-host:9090").build()));
        assertThrows(RetinaException.class, () -> checkpoint.load());
    }

    /** A body with a negative checkpointAppliedTs is corruption, not a fresh signal. */
    @Test
    public void testLoad_negativeCheckpointTs_failsClosed() throws IOException
    {
        stubPointer(BODY_PATH);
        stubBodyBytes(serialize(baseBuilder().checkpointAppliedTs(-1L).build()));
        assertThrows(RetinaException.class, () -> checkpoint.load());
    }

    // ===============================================================
    //   generate() — write body and publish pointer
    // ===============================================================

    @Test
    public void testGenerate_writesSortedBodyAndPublishesPointer() throws Exception
    {
        stubPointer(null);
        stubCreateCapturingBody();
        when(etcd.compareAndPut(eq(POINTER_KEY), eq(null), eq(generatedBodyPath(4000L)))).thenReturn(true);

        List<RecoveryCheckpoint.VisibilityEntry> rgs = new ArrayList<>(Arrays.asList(
                new RecoveryCheckpoint.VisibilityEntry(20L, 3, 30, 2200L, new long[]{0x20L}),
                new RecoveryCheckpoint.VisibilityEntry(10L, 2, 20, 2100L, new long[]{0x10L}),
                new RecoveryCheckpoint.VisibilityEntry(10L, 1, 10, 2000L, new long[]{0x01L})));
        List<RecoveryCheckpoint.PendingSegmentEntry> segments = new ArrayList<>(Arrays.asList(
                new RecoveryCheckpoint.PendingSegmentEntry(2, 900L),
                new RecoveryCheckpoint.PendingSegmentEntry(7, 370L),
                new RecoveryCheckpoint.PendingSegmentEntry(1, 310L)));

        checkpoint.generate(4000L, rgs, segments);

        assertEquals(Collections.singletonList(generatedBodyPath(4000L)), createdPaths);
        verify(etcd).compareAndPut(POINTER_KEY, null, generatedBodyPath(4000L));
        verify(storage, never()).delete(anyString(), eq(false));

        RecoveryCheckpoint.Body written = RecoveryCheckpoint.Body.readFrom(createdBodies.get(0).toByteArray());
        assertEquals(NODE_ID, written.getRetinaNodeId());
        assertEquals(4000L, written.getCheckpointAppliedTs());
        assertEquals(VNODES, written.getVirtualNodesPerNode());
        assertTrue("writeTimeMs should be populated", written.getWriteTimeMs() > 0L);

        assertSegmentEntry(written.getSegmentEntries().get(0), 1, 310L);
        assertSegmentEntry(written.getSegmentEntries().get(1), 2, 900L);
        assertSegmentEntry(written.getSegmentEntries().get(2), 7, 370L);
        assertRgEntry(written.getRgEntries().get(0), 10L, 1, 10, 2000L, new long[]{0x01L});
        assertRgEntry(written.getRgEntries().get(1), 10L, 2, 20, 2100L, new long[]{0x10L});
        assertRgEntry(written.getRgEntries().get(2), 20L, 3, 30, 2200L, new long[]{0x20L});
    }

    @Test
    public void testGenerate_deletesDisplacedBodyAfterPublish() throws Exception
    {
        stubPointer(BODY_PATH);
        stubCreateCapturingBody();
        when(etcd.compareAndPut(eq(POINTER_KEY), eq(BODY_PATH), eq(generatedBodyPath(4001L)))).thenReturn(true);

        checkpoint.generate(4001L, new ArrayList<>(), new ArrayList<>());

        verify(etcd).compareAndPut(POINTER_KEY, BODY_PATH, generatedBodyPath(4001L));
        verify(storage).delete(BODY_PATH, false);
    }

    @Test
    public void testGenerate_deleteDisplacedBodyFailure_doesNotFailPublish() throws Exception
    {
        stubPointer(BODY_PATH);
        stubCreateCapturingBody();
        when(etcd.compareAndPut(eq(POINTER_KEY), eq(BODY_PATH), eq(generatedBodyPath(4002L)))).thenReturn(true);
        when(storage.delete(BODY_PATH, false)).thenThrow(new IOException("delete failed"));

        checkpoint.generate(4002L, new ArrayList<>(), new ArrayList<>());

        verify(storage, times(1)).create(anyString(), eq(true), anyInt());
        verify(etcd, times(1)).compareAndPut(eq(POINTER_KEY), eq(BODY_PATH), eq(generatedBodyPath(4002L)));
        verify(storage).delete(BODY_PATH, false);
    }

    @Test
    public void testGenerate_sameTimestampSkipsAfterSuccessfulPublish() throws Exception
    {
        stubPointer(null);
        stubCreateCapturingBody();
        when(etcd.compareAndPut(eq(POINTER_KEY), eq(null), eq(generatedBodyPath(4003L)))).thenReturn(true);

        checkpoint.generate(4003L, new ArrayList<>(), new ArrayList<>());
        checkpoint.generate(4003L, new ArrayList<>(Collections.singletonList(
                new RecoveryCheckpoint.VisibilityEntry(1L, 0, 0, 0L, new long[0]))), new ArrayList<>());

        verify(storage, times(1)).create(anyString(), eq(true), anyInt());
        verify(etcd, times(1)).compareAndPut(eq(POINTER_KEY), eq(null), eq(generatedBodyPath(4003L)));
        assertEquals(1, createdBodies.size());
    }

    @Test
    public void testGenerate_writeFailureDoesNotPublishOrAdvanceTimestamp() throws Exception
    {
        stubPointer(null);
        when(storage.create(anyString(), eq(true), anyInt()))
                .thenThrow(new IOException("write failed"))
                .thenAnswer(captureCreate());
        when(etcd.compareAndPut(eq(POINTER_KEY), eq(null), eq(generatedBodyPath(4004L)))).thenReturn(true);

        assertThrows(RetinaException.class,
                () -> checkpoint.generate(4004L, new ArrayList<>(), new ArrayList<>()));
        checkpoint.generate(4004L, new ArrayList<>(), new ArrayList<>());

        verify(storage, times(2)).create(anyString(), eq(true), anyInt());
        verify(etcd, times(1)).compareAndPut(eq(POINTER_KEY), eq(null), eq(generatedBodyPath(4004L)));
    }

    @Test
    public void testGenerate_casFailureDoesNotAdvanceTimestamp() throws Exception
    {
        stubPointer(null);
        stubCreateCapturingBody();
        when(etcd.compareAndPut(eq(POINTER_KEY), eq(null), eq(generatedBodyPath(4005L))))
                .thenReturn(false)
                .thenReturn(true);

        assertThrows(RetinaException.class,
                () -> checkpoint.generate(4005L, new ArrayList<>(), new ArrayList<>()));
        checkpoint.generate(4005L, new ArrayList<>(), new ArrayList<>());

        verify(storage, times(2)).create(anyString(), eq(true), anyInt());
        verify(etcd, times(2)).compareAndPut(eq(POINTER_KEY), eq(null), eq(generatedBodyPath(4005L)));
    }
}
