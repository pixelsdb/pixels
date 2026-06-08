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
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.common.utils.NetUtils;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Single owner of the recovery-checkpoint contract for a Retina host:
 * binary format, value object ({@link Body} with its entry POJOs), and
 * the etcd-pointer + Storage IO protocol that publishes and loads
 * bodies. Catalog reconciliation, replay-start computation, and orphan
 * retirement are not this class's concern; see {@code RetinaServerImpl}.
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
 *       and a {@code virtualNodesPerNode} match). Returns {@code null}
 *       <em>only</em> when the pointer is absent (the sole legitimate
 *       fresh-deployment signal); once the pointer exists every failure
 *       fails closed by throwing, so a transient read error or corrupted
 *       body is never mistaken for "no checkpoint".</li>
 *   <li>{@link Body#writeTo(DataOutputStream)} / {@link Body#readFrom(byte[])} — the
 *       on-disk format codec; integrity is delegated to the underlying storage layer.</li>
 * </ul>
 */
public final class RecoveryCheckpoint
{
    private static final Logger logger = LogManager.getLogger(RecoveryCheckpoint.class);

    private static final int MAGIC = 0x5052434B;

    // ============================================================
    //   Section 1 — Configuration / IO state
    // ============================================================

    private final Storage storage;
    private final String checkpointDir;
    private final EtcdUtil etcd;
    private final int virtualNodesPerNode;
    private final String retinaNodeId;
    private final String pointerKey;
    /** Last checkpointAppliedTs that was successfully persisted; -1 before the first round. */
    private long lastCheckpointAppliedTs = -1L;

    RecoveryCheckpoint(Storage storage,
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
     * Build a recovery checkpoint from the running configuration (shared
     * {@link EtcdUtil#Instance()}, storage resolved from
     * {@code retina.recovery.checkpoint.dir}, vnode count from
     * {@code node.virtual.num}). The local hostname is used as the
     * per-host retinaNodeId.
     */
    public static RecoveryCheckpoint createFromConfig() throws RetinaException
    {
        ConfigFactory config = ConfigFactory.Instance();
        String retinaNodeId = NetUtils.getLocalHostName();
        String checkpointDir = config.getProperty("retina.recovery.checkpoint.dir");
        Storage storage;
        try
        {
            storage = StorageFactory.Instance().getStorage(checkpointDir);
        }
        catch (IOException e)
        {
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

    // ============================================================
    //   Section 2 — Entry POJOs serialised inside a body
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
        private final int virtualNodeId;
        private final long minCommitTs;

        public PendingSegmentEntry(int virtualNodeId, long minCommitTs)
        {
            this.virtualNodeId = virtualNodeId;
            this.minCommitTs = minCommitTs;
        }

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
    //   Section 3 — Body value object + format codec
    // ============================================================

    /**
     * Immutable in-memory representation of one checkpoint body.
     * Use {@link Body#builder()} to construct, {@link #writeTo(DataOutputStream)} to
     * write, and {@link #readFrom(byte[])} to parse.
     */
    public static final class Body
    {
        private final long writeTimeMs;
        private final long checkpointAppliedTs;
        /** Value of {@code node.virtual.num} at checkpoint time; mismatch aborts recovery. */
        private final int virtualNodesPerNode;
        /** Original retinaNodeId string, stored for diagnostics. */
        private final String retinaNodeId;

        private final List<PendingSegmentEntry> segmentEntries;
        private final List<VisibilityEntry> rgEntries;

        private Body(Builder builder)
        {
            this.writeTimeMs = builder.writeTimeMs;
            this.checkpointAppliedTs = builder.checkpointAppliedTs;
            this.virtualNodesPerNode = builder.virtualNodesPerNode;
            this.retinaNodeId = builder.retinaNodeId;
            this.segmentEntries = Collections.unmodifiableList(new ArrayList<>(
                    builder.segmentEntries == null ? Collections.emptyList() : builder.segmentEntries));
            this.rgEntries = Collections.unmodifiableList(new ArrayList<>(
                    builder.rgEntries == null ? Collections.emptyList() : builder.rgEntries));
        }

        public long getWriteTimeMs() { return writeTimeMs; }
        public long getCheckpointAppliedTs() { return checkpointAppliedTs; }
        public int getVirtualNodesPerNode() { return virtualNodesPerNode; }
        public String getRetinaNodeId() { return retinaNodeId; }
        public List<PendingSegmentEntry> getSegmentEntries() { return segmentEntries; }
        public List<VisibilityEntry> getRgEntries() { return rgEntries; }

        public void writeTo(DataOutputStream out) throws IOException
        {
            out.writeInt(MAGIC);
            out.writeLong(writeTimeMs);
            out.writeLong(checkpointAppliedTs);
            out.writeInt(virtualNodesPerNode);
            out.writeInt(segmentEntries.size());
            out.writeInt(rgEntries.size());

            byte[] nodeIdBytes = retinaNodeId.getBytes(StandardCharsets.UTF_8);
            out.writeInt(nodeIdBytes.length);
            out.write(nodeIdBytes);

            for (PendingSegmentEntry se : segmentEntries)
            {
                out.writeInt(se.virtualNodeId);
                out.writeLong(se.minCommitTs);
            }

            for (VisibilityEntry ve : rgEntries)
            {
                out.writeLong(ve.fileId);
                out.writeInt(ve.rgId);
                out.writeInt(ve.recordNum);
                out.writeLong(ve.baseTimestamp);
                long[] bitmap = ve.bitmap;
                int bitmapLen = bitmap == null ? 0 : bitmap.length;
                out.writeInt(bitmapLen);
                for (int i = 0; i < bitmapLen; i++)
                {
                    out.writeLong(bitmap[i]);
                }
            }
        }

        /**
         * Parse the supplied bytes. Throws {@link RetinaException} on
         * magic mismatch or malformed content.
         */
        public static Body readFrom(byte[] bytes) throws RetinaException
        {
            if (bytes == null || bytes.length == 0)
            {
                throw new RetinaException("body is empty");
            }

            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes)))
            {
                int magic = dis.readInt();
                if (magic != MAGIC)
                {
                    throw new RetinaException("bad magic: " + Integer.toHexString(magic));
                }
                long writeTimeMs = dis.readLong();
                long checkpointAppliedTs = dis.readLong();
                int virtualNodesPerNode = dis.readInt();
                int segmentEntryCount = dis.readInt();
                int rgEntryCount = dis.readInt();
                if (segmentEntryCount < 0 || rgEntryCount < 0)
                {
                    throw new RetinaException("negative entry counts");
                }

                int nodeIdLen = dis.readInt();
                if (nodeIdLen < 0 || nodeIdLen > 1024 || nodeIdLen > dis.available())
                {
                    throw new RetinaException("invalid retinaNodeId length: " + nodeIdLen);
                }
                byte[] nodeIdBytes = new byte[nodeIdLen];
                dis.readFully(nodeIdBytes);
                String retinaNodeId = new String(nodeIdBytes, StandardCharsets.UTF_8);

                List<PendingSegmentEntry> segments = new ArrayList<>();
                for (int i = 0; i < segmentEntryCount; i++)
                {
                    int virtualNodeId = dis.readInt();
                    long minCommitTs = dis.readLong();
                    segments.add(new PendingSegmentEntry(virtualNodeId, minCommitTs));
                }
                List<VisibilityEntry> rgs = new ArrayList<>();
                for (int i = 0; i < rgEntryCount; i++)
                {
                    long fileId = dis.readLong();
                    int rgId = dis.readInt();
                    int recordNum = dis.readInt();
                    long baseTimestamp = dis.readLong();
                    int bitmapLen = dis.readInt();
                    if (rgId < 0 || recordNum < 0 || bitmapLen < 0 || (long) bitmapLen * Long.BYTES > dis.available())
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
            private long checkpointAppliedTs;
            private int virtualNodesPerNode;
            private String retinaNodeId;
            private List<PendingSegmentEntry> segmentEntries = Collections.emptyList();
            private List<VisibilityEntry> rgEntries = Collections.emptyList();

            public Builder writeTimeMs(long writeTimeMs) { this.writeTimeMs = writeTimeMs; return this; }
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
    //   Section 4 — Load result
    // ============================================================

    /** Body loaded from the etcd pointer. */
    public static final class LoadedCheckpoint
    {
        public final String bodyPath;
        public final Body body;

        LoadedCheckpoint(String bodyPath, Body body)
        {
            this.bodyPath = bodyPath;
            this.body = body;
        }
    }

    // ============================================================
    //   Section 5 — Write path: generate()
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
     *         <p>A no-op when {@code checkpointAppliedTs} has not advanced since
     *         the last successful round (no new committed transactions, nothing
     *         to flush). The published body and pointer are logged at INFO.
     */
    public void generate(long checkpointAppliedTs,
                         List<VisibilityEntry> rgEntries,
                         List<PendingSegmentEntry> segments) throws RetinaException
    {
        if (checkpointAppliedTs == lastCheckpointAppliedTs)
        {
            logger.debug("Recovery checkpoint: checkpointAppliedTs={} unchanged since last round; skipping",
                    checkpointAppliedTs);
            return;
        }
        long now = System.currentTimeMillis();

        rgEntries.sort((a, b) -> {
            int byFile = Long.compare(a.getFileId(), b.getFileId());
            if (byFile != 0) return byFile;
            return Integer.compare(a.getRgId(), b.getRgId());
        });
        segments.sort((a, b) -> Integer.compare(a.getVirtualNodeId(), b.getVirtualNodeId()));

        Body body = Body.builder()
                .retinaNodeId(retinaNodeId)
                .writeTimeMs(now)
                .checkpointAppliedTs(checkpointAppliedTs)
                .virtualNodesPerNode(virtualNodesPerNode)
                .segmentEntries(segments)
                .rgEntries(rgEntries)
                .build();

        String bodyPath = RetinaUtils.buildCheckpointPath(
                checkpointDir, RetinaUtils.CHECKPOINT_PREFIX_RECOVERY, retinaNodeId, checkpointAppliedTs);
        try (DataOutputStream out = storage.create(bodyPath, true, Constants.CHECKPOINT_BUFFER_SIZE))
        {
            body.writeTo(out);
            out.flush();
        }
        catch (IOException e)
        {
            throw new RetinaException("Failed to write recovery checkpoint body " + bodyPath, e);
        }

        // Body is durable; publish the pointer atomically. If publish fails the
        // body becomes a one-round orphan and is overwritten/cleaned next round.
        String displacedPath = publishPointer(bodyPath);
        if (displacedPath != null && !displacedPath.isEmpty())
        {
            try
            {
                storage.delete(displacedPath, false);
            }
            catch (IOException e)
            {
                logger.warn("Failed to delete orphan checkpoint body {}; will retry next round",
                        displacedPath, e);
            }
        }

        logger.info("Recovery checkpoint published: body={}, checkpointAppliedTs={}, segments={}, rgs={}",
                bodyPath, checkpointAppliedTs,
                segments.size(), rgEntries.size());
        lastCheckpointAppliedTs = checkpointAppliedTs;
    }

    /**
     * Atomically replace the published checkpoint pointer.
     *
     * @return the displaced old body path (null on first publish).
     */
    private String publishPointer(String newBodyPath) throws RetinaException
    {
        String old = readPointer();
        boolean committed;
        try
        {
            committed = etcd.compareAndPut(pointerKey, old, newBodyPath);
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

    // ============================================================
    //   Section 6 — Read path: load()
    // ============================================================

    /**
     * Read the etcd pointer and load the body it references. Returns
     * {@code null} <em>only</em> when the pointer is absent, which is the
     * sole legitimate fresh-deployment signal (this node has never
     * checkpointed). Once the pointer exists a checkpoint was definitely
     * taken, so every subsequent failure — a transient read error, a
     * corrupted body, a stale vnode mapping, or any other unusable body —
     * fails closed by throwing. Recovery must abort and retry (or wait for
     * operator intervention) rather than silently rebuild from scratch,
     * which would wipe real visibility state and serve dirty reads while
     * CDC replay catches up.
     */
    public LoadedCheckpoint load() throws RetinaException
    {
        String bodyPath = readPointer();
        if (bodyPath == null || bodyPath.isEmpty())
        {
            return null;
        }
        byte[] bytes;
        try
        {
            bytes = readBody(bodyPath);
        }
        catch (IOException e)
        {
            // Fail-closed: the pointer exists, so a checkpoint was taken; a
            // transient read failure is not the same as "no checkpoint".
            // Abort and let recovery retry rather than fresh-deploy.
            throw new RetinaException(String.format(
                    "Recovery aborted: pointer references %s but reading the checkpoint body failed. "
                            + "Refusing to treat a transient read error as a fresh deployment.", bodyPath), e);
        }
        Body body;
        try
        {
            body = Body.readFrom(bytes);
        }
        catch (RetinaException e)
        {
            // Fail-closed: a corrupted/truncated body (e.g. a half-written
            // body from a power loss) must not be misread as "no checkpoint".
            throw new RetinaException(String.format(
                    "Recovery aborted: checkpoint body %s is corrupted/unreadable. "
                            + "Operator intervention required; refusing to fresh-deploy over a damaged checkpoint.",
                    bodyPath), e);
        }
        // Fail-closed: configuration changed since last checkpoint. Abort
        // recovery and let the operator intervene rather than rebuild with
        // a stale vnode mapping.
        if (body.getVirtualNodesPerNode() != virtualNodesPerNode)
        {
            throw new RetinaException(String.format(
                    "Recovery aborted: body %s was written with node.virtual.num=%d, current=%d. "
                            + "Configuration changed since last checkpoint; refusing to recover with stale vnode mapping.",
                    bodyPath, body.getVirtualNodesPerNode(), virtualNodesPerNode));
        }
        // Fail-closed: the pointer named this body, so it must be usable.
        // A mismatched node id or illegal timestamp is corruption, not a
        // fresh-deployment signal.
        ensureAcceptable(body, bodyPath);
        return new LoadedCheckpoint(bodyPath, body);
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

    private byte[] readBody(String path) throws IOException
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (DataInputStream in = storage.open(path))
        {
            byte[] chunk = new byte[8192];
            int n;
            while ((n = in.read(chunk)) != -1)
            {
                buf.write(chunk, 0, n);
            }
        }
        byte[] result = buf.toByteArray();
        if (result.length == 0)
        {
            throw new IOException("empty body file at " + path);
        }
        return result;
    }

    private void ensureAcceptable(Body body, String bodyPath) throws RetinaException
    {
        if (!retinaNodeId.equals(body.getRetinaNodeId()))
        {
            throw new RetinaException(String.format(
                    "Recovery aborted: body %s retinaNodeId='%s' does not match expected '%s'. "
                            + "Pointer references a body for a different node; refusing to fresh-deploy.",
                    bodyPath, body.getRetinaNodeId(), retinaNodeId));
        }
        if (body.getCheckpointAppliedTs() < 0)
        {
            throw new RetinaException(String.format(
                    "Recovery aborted: body %s has illegal checkpointAppliedTs=%d. "
                            + "Corrupted checkpoint; refusing to fresh-deploy.",
                    bodyPath, body.getCheckpointAppliedTs()));
        }
    }
}
