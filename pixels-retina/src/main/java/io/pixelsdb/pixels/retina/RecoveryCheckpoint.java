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

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.common.utils.NetUtils;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * Single owner of the recovery-checkpoint contract for a Retina host:
 * binary format, value object ({@link Body} with its entry POJOs), and
 * the etcd-pointer + Storage IO protocol that publishes and loads
 * bodies. Catalog reconciliation, replay-start computation, and orphan
 * retirement are not this class's concern; see {@link RecoveryProcedure}.
 * <p>
 * High-level surface:
 * <ul>
 *   <li>{@link #generate(long, List, List)} — given pre-collected
 *       {@code rgEntries} and {@code segments} captured by the caller at
 *       {@code checkpointAppliedTs}, sort canonically, serialise a body,
 *       write it through {@link Storage}, and publish the per-host etcd
 *       pointer via CAS. Idempotent across rounds: a no-op when
 *       {@code checkpointAppliedTs} has not advanced since the last
 *       successful round. Pure transform + IO; never reads back into RRM.</li>
 *   <li>{@link #load()} — read the etcd pointer, fetch the body it
 *       references, and run minimal header-level acceptability checks
 *       (matching {@code retinaNodeId}, sane {@code checkpointAppliedTs},
 *       and a fail-closed {@code virtualNodesPerNode} match). Returns
 *       {@code null} when the pointer is absent or the body is unusable
 *       so the caller can fall back to fresh-deployment handling.</li>
 *   <li>{@link Body#serialize()} / {@link Body#readFrom(byte[])} — the
 *       on-disk format codec; bytes route through {@link CRC32} and the
 *       loader rejects bodies whose trailer length or CRC disagrees.</li>
 * </ul>
 */
public final class RecoveryCheckpoint
{
    private static final Logger logger = LogManager.getLogger(RecoveryCheckpoint.class);

    // ============================================================
    //   Section 1 — On-disk format constants
    // ============================================================

    private static final int MAGIC = 0x5052434B;

    /** Body length (4) + CRC32 (4). */
    private static final int TRAILER_SIZE = 4 + 4;

    /** Initial buffer capacity hint; ByteArrayOutputStream grows as needed. */
    private static final int INITIAL_BUFFER_HINT = 4 * 1024;

    private static final int WRITE_BUFFER = 4 * 1024 * 1024;

    // ============================================================
    //   Section 2 — Configuration / IO state
    // ============================================================

    private final Storage storage;
    private final String checkpointDir;
    private final EtcdUtil etcd;
    private final int virtualNodesPerNode;
    private final String retinaNodeId;
    private final String pointerKey;
    /** Last checkpointAppliedTs that was successfully persisted; -1 before the first round. */
    private long lastFoldingTs = -1L;

    public RecoveryCheckpoint(Storage storage,
                              String checkpointDir,
                              EtcdUtil etcd,
                              int virtualNodesPerNode,
                              String retinaNodeId)
    {
        this.storage = storage;
        this.checkpointDir = checkpointDir;
        this.etcd = etcd;
        this.virtualNodesPerNode = virtualNodesPerNode;
        this.retinaNodeId = retinaNodeId;
        this.pointerKey = "/pixels/retina/recovery/checkpoint/" + retinaNodeId + "/current";
    }

    /**
     * Build a recovery checkpoint using the default wiring (service
     * singletons, shared {@link EtcdUtil#Instance()}, body storage resolved
     * from {@code retina.recovery.checkpoint.dir}). The local hostname is
     * used as the per-host retinaNodeId.
     */
    public static RecoveryCheckpoint createDefault() throws RetinaException
    {
        ConfigFactory config = ConfigFactory.Instance();
        String retinaNodeId = NetUtils.getLocalHostName();
        String dir = config.getProperty("retina.recovery.checkpoint.dir");
        String checkpointDir = trimTrailingSlash(dir);
        Storage storage;
        try {
            storage = StorageFactory.Instance().getStorage(checkpointDir);
        } catch (IOException e) {
            throw new RetinaException("Failed to resolve storage for " + checkpointDir, e);
        }
        int virtualNodesPerNode = Integer.parseInt(config.getProperty("node.virtual.num"));

        return new RecoveryCheckpoint(
                storage,
                checkpointDir,
                EtcdUtil.Instance(),
                virtualNodesPerNode,
                retinaNodeId);
    }

    public int getVirtualNodesPerNode()
    {
        return virtualNodesPerNode;
    }

    public String getRetinaNodeId()
    {
        return retinaNodeId;
    }

    // ============================================================
    //   Section 3 — Entry POJOs serialised inside a body
    // ============================================================

    /**
     * Per-scope earliest unsafe-insert commit timestamp captured at
     * checkpoint time: the smallest commit ts across the scope's
     * pending/open {@link io.pixelsdb.pixels.retina.FileWriterManager}s.
     * Already-published REGULAR files are not tracked separately in the
     * body; their {@code fileId} appears in {@link VisibilityEntry} and
     * that is the only ingest-path identity recovery needs.
     */
    public static final class PendingSegmentEntry
    {
        private final long tableId;
        private final int virtualNodeId;
        private final long minCommitTs;

        public PendingSegmentEntry(long tableId, int virtualNodeId, long minCommitTs)
        {
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
            this.minCommitTs = minCommitTs;
        }

        public long getTableId() { return tableId; }
        public int getVirtualNodeId() { return virtualNodeId; }
        public long getMinCommitTs() { return minCommitTs; }
    }

    /**
     * One {@code (fileId, rgId, bitmap)} entry captured by the recovery
     * checkpoint. The bitmap folds every delete with
     * {@code delete_ts <= baseTimestamp} into the base, so the loader can
     * rebuild RGVisibility with an empty deletion chain.
     */
    public static final class VisibilityEntry
    {
        private final long fileId;
        private final int rgId;
        private final int recordNum;
        private final long baseTimestamp;
        private final long[] bitmap;

        public VisibilityEntry(long fileId, int rgId, int recordNum,
                               long baseTimestamp, long[] bitmap)
        {
            this.fileId = fileId;
            this.rgId = rgId;
            this.recordNum = recordNum;
            this.baseTimestamp = baseTimestamp;
            this.bitmap = bitmap;
        }

        public long getFileId() { return fileId; }
        public int getRgId() { return rgId; }
        public int getRecordNum() { return recordNum; }
        public long getBaseTimestamp() { return baseTimestamp; }
        public long[] getBitmap() { return bitmap; }
    }

    // ============================================================
    //   Section 4 — Body value object + format codec
    // ============================================================

    /**
     * Immutable in-memory representation of one checkpoint body.
     * Use {@link Body#builder()} to construct, {@link #serialize()} to
     * write, and {@link #readFrom(byte[])} to parse; both routes thread
     * header+payload through {@link CRC32}.
     */
    public static final class Body
    {
        private final long writeTimeMs;
        private final long checkpointSnapshotTs;
        private final long checkpointAppliedTs;
        /** FNV-1a hash of {@code retinaNodeId = host:port}, used as a defence-in-depth check. */
        private final long retinaNodeIdHash;
        /** Value of {@code node.virtual.num} at checkpoint time; mismatch aborts recovery. */
        private final int virtualNodesPerNode;
        /** Original retinaNodeId string, stored for diagnostics. */
        private final String retinaNodeId;

        private final List<PendingSegmentEntry> segmentEntries;
        private final List<VisibilityEntry> rgEntries;

        private Body(Builder builder)
        {
            this.writeTimeMs = builder.writeTimeMs;
            this.checkpointSnapshotTs = builder.checkpointSnapshotTs;
            this.checkpointAppliedTs = builder.checkpointAppliedTs;
            this.retinaNodeIdHash = fnv1a64(builder.retinaNodeId);
            this.virtualNodesPerNode = builder.virtualNodesPerNode;
            this.retinaNodeId = builder.retinaNodeId;
            this.segmentEntries = Collections.unmodifiableList(new ArrayList<>(emptyIfNull(builder.segmentEntries)));
            this.rgEntries = Collections.unmodifiableList(new ArrayList<>(emptyIfNull(builder.rgEntries)));
        }

        public long getWriteTimeMs() { return writeTimeMs; }
        public long getCheckpointSnapshotTs() { return checkpointSnapshotTs; }
        public long getCheckpointAppliedTs() { return checkpointAppliedTs; }
        public long getRetinaNodeIdHash() { return retinaNodeIdHash; }
        public int getVirtualNodesPerNode() { return virtualNodesPerNode; }
        public String getRetinaNodeId() { return retinaNodeId; }
        public List<PendingSegmentEntry> getSegmentEntries() { return segmentEntries; }
        public List<VisibilityEntry> getRgEntries() { return rgEntries; }

        /**
         * Serialise this body and append the trailer (bodyLength + CRC32 over
         * header+payload bytes).
         */
        public byte[] serialize() throws IOException
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(INITIAL_BUFFER_HINT);
            CRC32 crc = new CRC32();
            CheckedOutputStream cos = new CheckedOutputStream(baos, crc);
            DataOutputStream dos = new DataOutputStream(cos);
            writeHeader(dos);
            writePayload(dos);
            dos.flush();

            int bodyLen = baos.size();
            long crcValue = crc.getValue();
            DataOutputStream trailerOut = new DataOutputStream(baos);
            trailerOut.writeInt(bodyLen);
            trailerOut.writeInt((int) (crcValue & 0xFFFFFFFFL));
            trailerOut.flush();
            return baos.toByteArray();
        }

        private void writeHeader(DataOutputStream dos) throws IOException
        {
            dos.writeInt(MAGIC);
            dos.writeLong(retinaNodeIdHash);
            dos.writeLong(writeTimeMs);
            dos.writeLong(checkpointSnapshotTs);
            dos.writeLong(checkpointAppliedTs);
            dos.writeInt(virtualNodesPerNode);
            dos.writeInt(segmentEntries.size());
            dos.writeInt(rgEntries.size());
        }

        private void writePayload(DataOutputStream dos) throws IOException
        {
            byte[] nodeIdBytes = retinaNodeId.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(nodeIdBytes.length);
            dos.write(nodeIdBytes);

            for (PendingSegmentEntry se : segmentEntries)
            {
                dos.writeLong(se.tableId);
                dos.writeInt(se.virtualNodeId);
                dos.writeLong(se.minCommitTs);
            }

            for (VisibilityEntry ve : rgEntries)
            {
                dos.writeLong(ve.fileId);
                dos.writeInt(ve.rgId);
                dos.writeInt(ve.recordNum);
                dos.writeLong(ve.baseTimestamp);
                long[] bitmap = ve.bitmap;
                int bitmapLen = bitmap == null ? 0 : bitmap.length;
                dos.writeInt(bitmapLen);
                for (int i = 0; i < bitmapLen; i++)
                {
                    dos.writeLong(bitmap[i]);
                }
            }
        }

        /**
         * Parse the supplied bytes. Throws {@link RetinaException} on
         * magic / version mismatch, truncated trailer, or CRC mismatch.
         */
        public static Body readFrom(byte[] bytes) throws RetinaException
        {
            if (bytes == null || bytes.length < TRAILER_SIZE)
            {
                throw new RetinaException("body too small: " + (bytes == null ? -1 : bytes.length));
            }

            int trailerOffset = bytes.length - TRAILER_SIZE;
            int declaredLen = readIntBE(bytes, trailerOffset);
            int declaredCrc = readIntBE(bytes, trailerOffset + 4);
            if (declaredLen != trailerOffset)
            {
                throw new RetinaException("trailer length mismatch: declared=" + declaredLen
                        + ", actual=" + trailerOffset);
            }
            CRC32 crc = new CRC32();
            crc.update(bytes, 0, trailerOffset);
            long expected = ((long) declaredCrc) & 0xFFFFFFFFL;
            if (crc.getValue() != expected)
            {
                throw new RetinaException("checksum mismatch: expected=" + expected
                        + ", actual=" + crc.getValue());
            }

            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes, 0, trailerOffset)))
            {
                int magic = dis.readInt();
                if (magic != MAGIC)
                {
                    throw new RetinaException("bad magic: " + Integer.toHexString(magic));
                }
                long retinaNodeIdHash = dis.readLong();
                long writeTimeMs = dis.readLong();
                long checkpointSnapshotTs = dis.readLong();
                long checkpointAppliedTs = dis.readLong();
                int virtualNodesPerNode = dis.readInt();
                int segmentEntryCount = dis.readInt();
                int rgEntryCount = dis.readInt();
                if (segmentEntryCount < 0 || rgEntryCount < 0)
                {
                    throw new RetinaException("negative entry counts");
                }

                int nodeIdLen = dis.readInt();
                if (nodeIdLen < 0 || nodeIdLen > dis.available())
                {
                    throw new RetinaException("invalid retinaNodeId length: " + nodeIdLen);
                }
                byte[] nodeIdBytes = new byte[nodeIdLen];
                dis.readFully(nodeIdBytes);
                String retinaNodeId = new String(nodeIdBytes, StandardCharsets.UTF_8);
                long computedHash = fnv1a64(retinaNodeId);
                if (computedHash != retinaNodeIdHash)
                {
                    throw new RetinaException("retinaNodeId hash mismatch: header="
                            + Long.toHexString(retinaNodeIdHash)
                            + ", body=" + Long.toHexString(computedHash));
                }

                List<PendingSegmentEntry> segments = new ArrayList<>();
                for (int i = 0; i < segmentEntryCount; i++)
                {
                    long tableId = dis.readLong();
                    int virtualNodeId = dis.readInt();
                    long minCommitTs = dis.readLong();
                    segments.add(new PendingSegmentEntry(tableId, virtualNodeId, minCommitTs));
                }
                List<VisibilityEntry> rgs = new ArrayList<>();
                for (int i = 0; i < rgEntryCount; i++)
                {
                    long fileId = dis.readLong();
                    int rgId = dis.readInt();
                    int recordNum = dis.readInt();
                    long baseTimestamp = dis.readLong();
                    int bitmapLen = dis.readInt();
                    if (rgId < 0 || recordNum <= 0 || bitmapLen < 0 || bitmapLen > dis.available() / Long.BYTES)
                    {
                        throw new RetinaException("invalid visibility entry for fileId=" + fileId
                                + ", rgId=" + rgId + ", recordNum=" + recordNum
                                + ", bitmapLen=" + bitmapLen);
                    }
                    long[] bitmap = new long[bitmapLen];
                    for (int j = 0; j < bitmapLen; j++)
                    {
                        bitmap[j] = dis.readLong();
                    }
                    rgs.add(new VisibilityEntry(fileId, rgId, recordNum, baseTimestamp, bitmap));
                }
                if (dis.available() != 0)
                {
                    throw new RetinaException("trailing bytes after checkpoint payload: " + dis.available());
                }

                return Body.builder()
                        .retinaNodeId(retinaNodeId)
                        .writeTimeMs(writeTimeMs)
                        .checkpointSnapshotTs(checkpointSnapshotTs)
                        .checkpointAppliedTs(checkpointAppliedTs)
                        .virtualNodesPerNode(virtualNodesPerNode)
                        .segmentEntries(segments)
                        .rgEntries(rgs)
                        .build();
            }
            catch (IOException e)
            {
                throw new RetinaException("failed to parse body", e);
            }
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static final class Builder
        {
            private long writeTimeMs;
            private long checkpointSnapshotTs;
            private long checkpointAppliedTs;
            private int virtualNodesPerNode;
            private String retinaNodeId;
            private List<PendingSegmentEntry> segmentEntries = Collections.emptyList();
            private List<VisibilityEntry> rgEntries = Collections.emptyList();

            public Builder writeTimeMs(long writeTimeMs) { this.writeTimeMs = writeTimeMs; return this; }
            public Builder checkpointSnapshotTs(long ts) { this.checkpointSnapshotTs = ts; return this; }
            public Builder checkpointAppliedTs(long ts) { this.checkpointAppliedTs = ts; return this; }
            public Builder virtualNodesPerNode(int n) { this.virtualNodesPerNode = n; return this; }
            public Builder retinaNodeId(String id) { this.retinaNodeId = id; return this; }
            public Builder segmentEntries(List<PendingSegmentEntry> entries) { this.segmentEntries = entries; return this; }
            public Builder rgEntries(List<VisibilityEntry> entries) { this.rgEntries = entries; return this; }

            public Body build()
            {
                if (retinaNodeId == null || retinaNodeId.isEmpty())
                {
                    throw new IllegalArgumentException("retinaNodeId is required");
                }
                return new Body(this);
            }
        }
    }

    // ============================================================
    //   Section 5 — Round / load results
    // ============================================================

    /** Result of one successful checkpoint round. */
    public static final class Result
    {
        private final String bodyObjectName;
        private final long checkpointAppliedTs;
        private final int segmentEntryCount;
        private final int rgEntryCount;

        public Result(String bodyObjectName, long checkpointAppliedTs,
                      int segmentEntryCount, int rgEntryCount)
        {
            this.bodyObjectName = bodyObjectName;
            this.checkpointAppliedTs = checkpointAppliedTs;
            this.segmentEntryCount = segmentEntryCount;
            this.rgEntryCount = rgEntryCount;
        }

        public String getBodyObjectName() { return bodyObjectName; }
        public long getCheckpointAppliedTs() { return checkpointAppliedTs; }
        public int getSegmentEntryCount() { return segmentEntryCount; }
        public int getRgEntryCount() { return rgEntryCount; }
    }

    /** Body loaded from the etcd pointer. */
    public static final class LoadedCheckpoint
    {
        public final String bodyObjectName;
        public final Body body;

        LoadedCheckpoint(String bodyObjectName, Body body)
        {
            this.bodyObjectName = bodyObjectName;
            this.body = body;
        }
    }

    // ============================================================
    //   Section 6 — Write path: generate()
    // ============================================================

    /**
     * @param checkpointAppliedTs the safe visibility folding timestamp at which
     *         the body should be snapshotted; supplied by the caller (typically
     *         the same value the surrounding GC cycle has just folded against)
     *         so the body reflects exactly that fold and TransService is not
     *         re-read here.
     * @param rgEntries per-RG visibility entries already snapshotted by the
     *         caller against {@code checkpointAppliedTs} (typically collected
     *         in-line during Memory GC's single pass over RGVisibility, so the
     *         post-fold bitmap is reused without a second native traversal).
     *         Sorted in-place to the canonical on-disk order.
     * @param segments per-scope earliest pending commit timestamps already
     *         snapshotted by the caller. Sorted in-place.
     * @return result of this checkpoint round, or {@code null} when
     *         {@code checkpointAppliedTs} has not advanced since the last
     *         successful round (no new committed transactions, nothing to flush).
     */
    public Result generate(long checkpointAppliedTs,
                           List<VisibilityEntry> rgEntries,
                           List<PendingSegmentEntry> segments) throws RetinaException
    {
        if (checkpointAppliedTs == lastFoldingTs)
        {
            logger.debug("Recovery checkpoint: checkpointAppliedTs={} unchanged since last round; skipping",
                    checkpointAppliedTs);
            return null;
        }
        long now = System.currentTimeMillis();

        rgEntries.sort((a, b) -> {
            int byFile = Long.compare(a.getFileId(), b.getFileId());
            if (byFile != 0) return byFile;
            return Integer.compare(a.getRgId(), b.getRgId());
        });
        sortSegments(segments);

        Body body = Body.builder()
                .retinaNodeId(retinaNodeId)
                .writeTimeMs(now)
                .checkpointSnapshotTs(now)
                .checkpointAppliedTs(checkpointAppliedTs)
                .virtualNodesPerNode(virtualNodesPerNode)
                .segmentEntries(segments)
                .rgEntries(rgEntries)
                .build();

        String bodyObjectName = RetinaUtils.getCheckpointFileName(
                RetinaUtils.CHECKPOINT_PREFIX_RECOVERY, retinaNodeId, checkpointAppliedTs);
        String bodyPath = checkpointDir + "/" + bodyObjectName;
        try
        {
            byte[] serialised = body.serialize();
            try (DataOutputStream out = storage.create(bodyPath, true, WRITE_BUFFER))
            {
                out.write(serialised);
                out.flush();
            }
        }
        catch (IOException e)
        {
            throw new RetinaException("Failed to write recovery checkpoint body " + bodyObjectName, e);
        }

        // Body is durable; publish the pointer atomically. If publish fails the
        // body becomes a one-round orphan and is overwritten/cleaned next round.
        String displacedOld = publishPointer(bodyObjectName);
        if (displacedOld != null && !displacedOld.isEmpty())
        {
            String displacedPath = checkpointDir + "/" + displacedOld;
            try
            {
                if (storage.exists(displacedPath))
                {
                    storage.delete(displacedPath, false);
                }
            }
            catch (IOException e)
            {
                logger.warn("Failed to delete orphan checkpoint body {} under {}; will retry next round",
                        displacedOld, checkpointDir, e);
            }
        }

        logger.info("Recovery checkpoint published: body={}, checkpointAppliedTs={}, segments={}, rgs={}",
                bodyObjectName, checkpointAppliedTs,
                segments.size(), rgEntries.size());
        lastFoldingTs = checkpointAppliedTs;
        return new Result(bodyObjectName, checkpointAppliedTs,
                segments.size(), rgEntries.size());
    }

    /**
     * Atomically replace the published checkpoint pointer.
     *
     * @return the displaced old body name (null on first publish).
     */
    private String publishPointer(String newBodyName) throws RetinaException
    {
        String old = readPointer();
        boolean committed;
        try
        {
            committed = etcd.compareAndPut(pointerKey, old, newBodyName);
        }
        catch (Exception e)
        {
            throw new RetinaException("etcd CAS failed for recovery checkpoint pointer " + pointerKey, e);
        }
        if (!committed)
        {
            throw new RetinaException("concurrent writer or stale snapshot on recovery checkpoint pointer "
                    + pointerKey);
        }
        return old;
    }

    private static void sortSegments(List<PendingSegmentEntry> segments)
    {
        segments.sort((a, b) -> {
            int byTable = Long.compare(a.getTableId(), b.getTableId());
            if (byTable != 0) return byTable;
            return Integer.compare(a.getVirtualNodeId(), b.getVirtualNodeId());
        });
    }

    // ============================================================
    //   Section 7 — Read path: load()
    // ============================================================

    /**
     * Read the etcd pointer and load the body it references. Returns
     * {@code null} when the pointer is absent or the body is unusable
     * (caller falls back to fresh-deployment handling). Throws when the
     * body's {@code virtualNodesPerNode} disagrees with the local config:
     * recovery must fail closed rather than rebuild with a stale vnode
     * mapping.
     */
    public LoadedCheckpoint load() throws RetinaException
    {
        String bodyName = readPointer();
        if (bodyName == null || bodyName.isEmpty())
        {
            return null;
        }
        byte[] bytes;
        try
        {
            bytes = readBody(bodyName);
        }
        catch (IOException e)
        {
            logger.warn("Recovery loader: pointer references {} but read failed", bodyName, e);
            return null;
        }
        Body body;
        try
        {
            body = Body.readFrom(bytes);
        }
        catch (RetinaException e)
        {
            logger.warn("Recovery loader: body {} is corrupted/unreadable", bodyName, e);
            return null;
        }
        // Fail-closed: configuration changed since last checkpoint. Abort
        // recovery and let the operator intervene rather than rebuild with
        // a stale vnode mapping.
        if (body.getVirtualNodesPerNode() != virtualNodesPerNode)
        {
            throw new RetinaException(String.format(
                    "Recovery aborted: body %s was written with node.virtual.num=%d, current=%d. "
                            + "Configuration changed since last checkpoint; refusing to recover with stale vnode mapping.",
                    bodyName, body.getVirtualNodesPerNode(), virtualNodesPerNode));
        }
        if (!isAcceptable(body, bodyName))
        {
            return null;
        }
        return new LoadedCheckpoint(bodyName, body);
    }

    private String readPointer()
    {
        KeyValue kv = etcd.getKeyValue(pointerKey);
        if (kv == null)
        {
            return null;
        }
        String value = kv.getValue().toString(StandardCharsets.UTF_8);
        return value.isEmpty() ? null : value;
    }

    private byte[] readBody(String objectName) throws IOException
    {
        String path = checkpointDir + "/" + objectName;
        long length = storage.getStatus(path).getLength();
        if (length <= 0)
        {
            throw new IOException("empty body file at " + path);
        }
        if (length > Integer.MAX_VALUE)
        {
            throw new IOException("body too large to read into memory: " + length + " bytes at " + path);
        }
        byte[] result = new byte[(int) length];
        try (DataInputStream in = storage.open(path))
        {
            in.readFully(result);
        }
        return result;
    }

    private boolean isAcceptable(Body body, String bodyName)
    {
        if (!retinaNodeId.equals(body.getRetinaNodeId()))
        {
            logger.warn("Recovery loader: body {} retinaNodeId='{}' does not match expected '{}'",
                    bodyName, body.getRetinaNodeId(), retinaNodeId);
            return false;
        }
        if (body.getCheckpointAppliedTs() < 0)
        {
            logger.warn("Recovery loader: body {} has illegal checkpointAppliedTs={}",
                    bodyName, body.getCheckpointAppliedTs());
            return false;
        }
        return true;
    }

    // ============================================================
    //   Section 8 — Misc helpers
    // ============================================================

    /** FNV-1a 64-bit hash, used for {@code retinaNodeId}. */
    static long fnv1a64(String s)
    {
        long hash = 0xcbf29ce484222325L;
        if (s == null)
        {
            return hash;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        for (byte b : bytes)
        {
            hash ^= (b & 0xFFL);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private static int readIntBE(byte[] arr, int off)
    {
        return ((arr[off] & 0xFF) << 24)
                | ((arr[off + 1] & 0xFF) << 16)
                | ((arr[off + 2] & 0xFF) << 8)
                | (arr[off + 3] & 0xFF);
    }

    private static <T> List<T> emptyIfNull(List<T> values)
    {
        return values == null ? Collections.emptyList() : values;
    }

    private static String trimTrailingSlash(String dir)
    {
        int len = dir.length();
        while (len > 0 && dir.charAt(len - 1) == '/')
        {
            len--;
        }
        return dir.substring(0, len);
    }
}
