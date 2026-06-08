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

import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.RollbackEntry;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.index.IndexProto;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Append-only write-ahead log for Storage GC rewrite operations.
 *
 * <p>Each GC task is stored as a single WAL file under {@code retina.storage.gc.journal.dir}.
 * The file is a stream of length-prefixed records:
 * <pre>
 *   [type: 1 byte][payloadLen: 4 bytes][payload: payloadLen bytes] ...
 * </pre>
 * Record types:
 * <ul>
 *   <li>{@code CREATE (1)}: written once when the task is opened; carries all task metadata.</li>
 *   <li>{@code ROLLBACK_ENTRY (2)}: appended once per kept row's primary-key switch,
 *       before the batch index update, so that recovery can restore the old rowId.</li>
 *   <li>{@code STATE_TRANSITION (3)}: appended when the task reaches a terminal or
 *       intermediate state.</li>
 * </ul>
 *
 * <p>State machine: {@code INDEX_SWITCHING → SWAPPED_NOT_CHECKPOINTED → CHECKPOINTED}
 *                                          {@code ↘ ABORTED ←────────────────────────}
 *
 * <p>A truncated record at the end of a file (crash mid-write) is silently dropped
 * during replay; the task state is reconstructed from the last complete record.
 *
 * <p>This class is not thread-safe for concurrent read/write access. Callers must
 * serialize WAL mutation, replay, and deletion for the WAL directory. Current
 * production use relies on startup recovery finishing before background GC starts,
 * and on the background GC cycle running WAL writes and reconciliation in one
 * single-threaded executor.
 */
public final class StorageGcWal
{
    private static final Logger logger = LogManager.getLogger(StorageGcWal.class);

    // ─── Record type codes ──────────────────────────────────────────────────
    static final byte RT_CREATE           = 1;
    static final byte RT_ROLLBACK_ENTRY   = 2;
    static final byte RT_STATE_TRANSITION = 3;

    // ─── State ──────────────────────────────────────────────────────────────

    public enum State
    {
        /** Primary index entries are being switched from old to new rowIds. */
        INDEX_SWITCHING((byte) 1),
        /** All index switches done, but the new file is not yet in a recovery checkpoint. */
        SWAPPED_NOT_CHECKPOINTED((byte) 2),
        /** New file confirmed in the checkpoint baseline; the rewrite is permanently committed. */
        CHECKPOINTED((byte) 3),
        /** Rolled back; old files and index entries have been restored. */
        ABORTED((byte) 4);

        final byte code;

        State(byte code)
        {
            this.code = code;
        }

        static State fromCode(byte code)
        {
            for (State s : values())
            {
                if (s.code == code)
                {
                    return s;
                }
            }
            throw new IllegalArgumentException("Unknown WAL state code: " + code);
        }

        public boolean isPending()
        {
            return this == INDEX_SWITCHING || this == SWAPPED_NOT_CHECKPOINTED;
        }

        public boolean isTerminal()
        {
            return this == CHECKPOINTED || this == ABORTED;
        }
    }

    // ─── Task (read-only view, reconstructed from WAL replay) ───────────────

    public static final class Task
    {
        private final String taskId;
        private final long tableId;
        private final int virtualNodeId;
        private final List<Long> oldFileIds;
        private final long newFileId;
        private final String newFilePath;
        private final long newRowIdStart;
        private final int newRowCount;
        private final List<RollbackEntry> rollbackEntries;
        private final State state;

        Task(String taskId, long tableId, int virtualNodeId,
             List<Long> oldFileIds, long newFileId, String newFilePath,
             long newRowIdStart, int newRowCount,
             List<RollbackEntry> rollbackEntries, State state)
        {
            this.taskId = taskId;
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
            this.oldFileIds = Collections.unmodifiableList(new ArrayList<>(oldFileIds));
            this.newFileId = newFileId;
            this.newFilePath = newFilePath == null ? "" : newFilePath;
            this.newRowIdStart = newRowIdStart;
            this.newRowCount = newRowCount;
            this.rollbackEntries = Collections.unmodifiableList(new ArrayList<>(rollbackEntries));
            this.state = state;
        }

        public String getTaskId() { return taskId; }
        public long getTableId() { return tableId; }
        public int getVirtualNodeId() { return virtualNodeId; }
        public List<Long> getOldFileIds() { return oldFileIds; }
        public long getNewFileId() { return newFileId; }
        public String getNewFilePath() { return newFilePath; }
        public long getNewRowIdStart() { return newRowIdStart; }
        public int getNewRowCount() { return newRowCount; }
        public List<RollbackEntry> getRollbackEntries() { return rollbackEntries; }
        public State getState() { return state; }
    }

    // ─── Writer (per-task write handle) ─────────────────────────────────────

    /**
     * Write handle for one GC task's WAL file. Holds an open append stream for the
     * duration of the {@code syncIndex} → {@code commitFileGroup/rollback} window.
     * Calling {@link #markSwapped()} or {@link #markAborted()} writes the final state
     * record and closes the stream automatically.
     */
    public static final class Writer implements Closeable
    {
        private final String taskId;
        private DataOutputStream out;

        Writer(String taskId, DataOutputStream out)
        {
            this.taskId = taskId;
            this.out = out;
        }

        public String getTaskId()
        {
            return taskId;
        }

        /**
         * Appends one rollback entry. Must be called before the corresponding index
         * pointer is updated so that recovery can restore the old rowId.
         */
        public void appendRollbackEntry(IndexProto.IndexKey indexKey,
                                        long oldRowId, long newRowId) throws IOException
        {
            byte[] keyBytes = indexKey.toByteArray();
            int payloadLen = 4 + keyBytes.length + 16;
            out.writeByte(RT_ROLLBACK_ENTRY);
            out.writeInt(payloadLen);
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
            out.writeLong(oldRowId);
            out.writeLong(newRowId);
        }

        /**
         * Flushes buffered data to the underlying storage. Call this after all
         * rollback entries have been appended and before the batch index update,
         * to ensure crash-safety of the undo log.
         */
        public void flush() throws IOException
        {
            out.flush();
        }

        /** Writes {@code SWAPPED_NOT_CHECKPOINTED} record and closes the stream. */
        public void markSwapped() throws IOException
        {
            writeStateTransition(out, State.SWAPPED_NOT_CHECKPOINTED);
            close();
        }

        /** Writes {@code ABORTED} record and closes the stream. */
        public void markAborted() throws IOException
        {
            writeStateTransition(out, State.ABORTED);
            close();
        }

        @Override
        public void close() throws IOException
        {
            if (out != null)
            {
                out.flush();
                out.close();
                out = null;
            }
        }
    }

    // ─── Fields & constructor ────────────────────────────────────────────────

    private final Storage storage;
    private final String walDir;

    public StorageGcWal()
    {
        this.walDir = ConfigFactory.Instance().getProperty("retina.storage.gc.journal.dir");
        try
        {
            this.storage = StorageFactory.Instance().getStorage(walDir);
        }
        catch (IOException e)
        {
            throw new IllegalStateException(
                    "Failed to initialize Storage GC WAL at " + walDir, e);
        }
    }

    // ─── Write path (GC hot path) ────────────────────────────────────────────

    /**
     * Creates a new WAL file for a task, writes the {@code CREATE} record, and
     * returns an open {@link Writer} that the caller uses to append rollback entries
     * and mark the final task state.
     */
    public Writer createTask(String taskId, long tableId, int virtualNodeId,
                             List<Long> oldFileIds, long newFileId, String newFilePath,
                             long newRowIdStart, int newRowCount) throws IOException
    {
        String path = walPath(taskId);
        DataOutputStream out = storage.create(path, false,
                Constants.STORAGE_GC_JOURNAL_BUFFER_SIZE);
        try
        {
            writeCreateRecord(out, taskId, tableId, virtualNodeId, oldFileIds,
                    newFileId, newFilePath, newRowIdStart, newRowCount);
            out.flush();
        }
        catch (IOException e)
        {
            try { out.close(); } catch (IOException ignored) {}
            throw e;
        }
        return new Writer(taskId, out);
    }

    // ─── Cold-path state transitions (reconciliation / recovery) ────────────

    /** Appends a {@code CHECKPOINTED} record to an existing task's WAL file. */
    public void markCheckpointed(String taskId) throws IOException
    {
        appendStateTransition(taskId, State.CHECKPOINTED);
    }

    /** Appends an {@code ABORTED} record to an existing task's WAL file. */
    void markAborted(String taskId) throws IOException
    {
        appendStateTransition(taskId, State.ABORTED);
    }

    private void appendStateTransition(String taskId, State state) throws IOException
    {
        try (DataOutputStream out = storage.append(walPath(taskId),
                Constants.STORAGE_GC_JOURNAL_BUFFER_SIZE))
        {
            writeStateTransition(out, state);
            out.flush();
        }
    }

    // ─── Query path ──────────────────────────────────────────────────────────

    /** Returns all tasks by replaying every WAL file in the WAL directory. */
    public List<Task> listAllTasks()
    {
        try
        {
            if (!storage.exists(walDir))
            {
                return Collections.emptyList();
            }
            List<Task> tasks = new ArrayList<>();
            for (String path : storage.listPaths(walDir))
            {
                if (path == null || !path.endsWith(RetinaUtils.STORAGE_GC_JOURNAL_SUFFIX))
                {
                    continue;
                }
                try
                {
                    Task task = replayWal(path);
                    if (task != null)
                    {
                        tasks.add(task);
                    }
                }
                catch (IOException | RuntimeException e)
                {
                    throw new IllegalStateException(
                            "Corrupted Storage GC WAL file at " + path
                            + ". Delete this file to allow recovery to proceed.", e);
                }
            }
            return Collections.unmodifiableList(tasks);
        }
        catch (IOException e)
        {
            throw new IllegalStateException(
                    "Failed to list Storage GC WAL files under " + walDir, e);
        }
    }

    /** Returns the task for the given ID, or empty if the WAL file does not exist. */
    public Optional<Task> getTask(String taskId)
    {
        String path = walPath(taskId);
        try
        {
            if (!storage.exists(path))
            {
                return Optional.empty();
            }
            Task task = replayWal(path);
            return Optional.ofNullable(task);
        }
        catch (IOException e)
        {
            throw new IllegalStateException(
                    "Failed to read Storage GC WAL for taskId=" + taskId, e);
        }
    }

    /** Returns all file IDs referenced by tasks in a pending (non-terminal) state. */
    public Set<Long> collectPendingFileIds()
    {
        Set<Long> fileIds = new HashSet<>();
        for (Task task : listAllTasks())
        {
            if (task.getState().isPending())
            {
                fileIds.add(task.getNewFileId());
                fileIds.addAll(task.getOldFileIds());
            }
        }
        return Collections.unmodifiableSet(fileIds);
    }

    /** Returns only tasks in a terminal state (CHECKPOINTED or ABORTED). */
    public List<Task> listTerminalTasks()
    {
        return listAllTasks().stream()
                .filter(t -> t.getState().isTerminal())
                .collect(Collectors.toList());
    }

    // ─── Management ──────────────────────────────────────────────────────────

    /**
     * Deletes the WAL files for the given task IDs. Only terminal tasks may be deleted;
     * calling this with a non-terminal task ID throws {@link IllegalArgumentException}.
     */
    public void deleteTerminalTasks(Collection<String> taskIds)
    {
        if (taskIds == null || taskIds.isEmpty())
        {
            return;
        }
        for (String taskId : taskIds)
        {
            Optional<Task> task = getTask(taskId);
            if (!task.isPresent())
            {
                continue;
            }
            if (!task.get().getState().isTerminal())
            {
                throw new IllegalArgumentException(
                        "Cannot delete non-terminal WAL task " + taskId
                        + " in state " + task.get().getState());
            }
            String path = walPath(taskId);
            try
            {
                if (storage.exists(path))
                {
                    storage.delete(path, false);
                }
            }
            catch (IOException e)
            {
                throw new IllegalStateException(
                        "Failed to delete Storage GC WAL file for taskId=" + taskId, e);
            }
        }
    }

    // ─── Internal record I/O ─────────────────────────────────────────────────

    private static void writeCreateRecord(DataOutputStream out,
                                          String taskId, long tableId, int virtualNodeId,
                                          List<Long> oldFileIds, long newFileId,
                                          String newFilePath, long newRowIdStart,
                                          int newRowCount) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
        DataOutputStream payload = new DataOutputStream(baos);
        payload.writeUTF(taskId);
        payload.writeLong(tableId);
        payload.writeInt(virtualNodeId);
        payload.writeInt(oldFileIds.size());
        for (Long id : oldFileIds)
        {
            payload.writeLong(id);
        }
        payload.writeLong(newFileId);
        payload.writeUTF(newFilePath == null ? "" : newFilePath);
        payload.writeLong(newRowIdStart);
        payload.writeInt(newRowCount);
        byte[] bytes = baos.toByteArray();
        out.writeByte(RT_CREATE);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeStateTransition(DataOutputStream out, State state) throws IOException
    {
        out.writeByte(RT_STATE_TRANSITION);
        out.writeInt(1);
        out.writeByte(state.code);
    }

    /**
     * Replays a WAL file and returns the reconstructed {@link Task}, or {@code null} if the
     * file is empty. Throws {@link IllegalStateException} if the CREATE record is missing or
     * truncated (caller wraps this for the operator-friendly error message).
     */
    private Task replayWal(String path) throws IOException
    {
        try (DataInputStream in = storage.open(path))
        {
            // ── CREATE record (mandatory, must be first) ──────────────────
            int typeByte = in.read();
            if (typeByte == -1)
            {
                throw new IllegalStateException("Empty WAL file: no records");
            }
            if ((byte) typeByte != RT_CREATE)
            {
                throw new IllegalStateException(
                        "First record must be CREATE, got type " + typeByte);
            }
            int createLen;
            byte[] createPayload;
            try
            {
                createLen = in.readInt();
                createPayload = new byte[createLen];
                in.readFully(createPayload);
            }
            catch (EOFException e)
            {
                throw new IllegalStateException("Truncated CREATE record", e);
            }

            DataInputStream cp = new DataInputStream(
                    new ByteArrayInputStream(createPayload));
            String taskId      = cp.readUTF();
            long tableId       = cp.readLong();
            int virtualNodeId  = cp.readInt();
            int oldFileCount   = cp.readInt();
            List<Long> oldFileIds = new ArrayList<>(oldFileCount);
            for (int i = 0; i < oldFileCount; i++)
            {
                oldFileIds.add(cp.readLong());
            }
            long newFileId     = cp.readLong();
            String newFilePath = cp.readUTF();
            long newRowIdStart = cp.readLong();
            int newRowCount    = cp.readInt();

            // ── Subsequent records (rollback entries + state transitions) ──
            List<RollbackEntry> rollbackEntries = new ArrayList<>();
            State state = State.INDEX_SWITCHING;

            while (true)
            {
                int rt = in.read();
                if (rt == -1)
                {
                    break; // normal EOF
                }
                int rLen;
                byte[] rPayload;
                try
                {
                    rLen    = in.readInt();
                    rPayload = new byte[rLen];
                    in.readFully(rPayload);
                }
                catch (EOFException e)
                {
                    logger.warn("WAL file {} has a truncated record of type {} at the end " +
                            "(possible crash mid-write); ignoring partial record", path, rt);
                    break;
                }

                if ((byte) rt == RT_ROLLBACK_ENTRY)
                {
                    DataInputStream re = new DataInputStream(
                            new ByteArrayInputStream(rPayload));
                    int keyLen = re.readInt();
                    byte[] keyBytes = new byte[keyLen];
                    re.readFully(keyBytes);
                    long oldRowId = re.readLong();
                    long newRowId = re.readLong();
                    rollbackEntries.add(new RollbackEntry(
                            IndexProto.IndexKey.parseFrom(keyBytes), oldRowId, newRowId));
                }
                else if ((byte) rt == RT_STATE_TRANSITION)
                {
                    state = State.fromCode(rPayload[0]);
                }
                else
                {
                    logger.warn("WAL file {} has unknown record type {}, skipping", path, rt);
                }
            }

            return new Task(taskId, tableId, virtualNodeId, oldFileIds,
                    newFileId, newFilePath, newRowIdStart, newRowCount,
                    rollbackEntries, state);
        }
    }

    private String walPath(String taskId)
    {
        return RetinaUtils.buildStorageGcJournalPath(walDir, taskId);
    }

    // ─── RecoveryHandler ─────────────────────────────────────────────────────

    public static final class RecoveryHandler
    {
        private final StorageGcWal wal;
        private final MetadataService metadataService;
        private final IndexService indexService;

        public RecoveryHandler(StorageGcWal wal,
                               MetadataService metadataService,
                               IndexService indexService)
        {
            this.wal             = Objects.requireNonNull(wal, "wal");
            this.metadataService = Objects.requireNonNull(metadataService, "metadataService");
            this.indexService    = Objects.requireNonNull(indexService, "indexService");
        }

        public void recover(Set<Long> baselineVisibleFileIds) throws RetinaException
        {
            Set<Long> baseline = baselineVisibleFileIds == null
                    ? Collections.emptySet() : baselineVisibleFileIds;

            // A single pass over all tasks. The durable truth is the checkpoint baseline
            // (which newFileIds are visible) plus the file catalog (REGULAR/RETIRED); the
            // WAL state only records how far the non-atomic rewrite progressed. For a
            // pending task the decision is uniform: if its newFile made it into the
            // baseline the rewrite is committed → advance to CHECKPOINTED; otherwise roll
            // back, restoring old files only when the catalog confirms the swap committed.
            for (Task task : wal.listAllTasks())
            {
                switch (task.getState())
                {
                    case CHECKPOINTED:
                        if (!baseline.contains(task.getNewFileId()))
                        {
                            throw new RetinaException(
                                    "Storage GC WAL recovery failed: CHECKPOINTED task "
                                    + task.getTaskId() + " newFileId=" + task.getNewFileId()
                                    + " is absent from selected checkpoint baseline");
                        }
                        break;
                    case ABORTED:
                        // Re-run unconditionally: the live rollback swallows per-step
                        // failures before writing ABORTED, so cleanup may be partial.
                        // cleanupNewFile is idempotent, so repeating it until
                        // deleteTerminalTasks removes the WAL is safe.
                        cleanupNewFile(task);
                        break;
                    case SWAPPED_NOT_CHECKPOINTED:
                    case INDEX_SWITCHING:
                        if (baseline.contains(task.getNewFileId()))
                        {
                            // Invariant: a checkpoint baseline can only contain a newFile
                            // whose task already reached SWAPPED_NOT_CHECKPOINTED. The GC
                            // cycle publishes the checkpoint (RRM.runGC Step 3) strictly
                            // after runStorageGC returns (Step 2), by which point every task
                            // has been marked SWAPPED or ABORTED; a crash inside the
                            // swap→markSwapped window aborts that whole cycle before any
                            // checkpoint is published. An INDEX_SWITCHING task in the
                            // baseline therefore means that ordering invariant was broken
                            // (e.g. storage GC made asynchronous, or checkpoint generation
                            // reordered ahead of the swap). Fail fast rather than committing
                            // a half-switched primary index.
                            if (task.getState() == State.INDEX_SWITCHING)
                            {
                                throw new RetinaException(
                                        "Storage GC WAL recovery failed: INDEX_SWITCHING task "
                                        + task.getTaskId() + " newFileId=" + task.getNewFileId()
                                        + " is present in the checkpoint baseline before its "
                                        + "index switch was marked complete (invariant violation)");
                            }
                            try { wal.markCheckpointed(task.getTaskId()); }
                            catch (IOException e)
                            {
                                throw new RetinaException(
                                        "WAL recovery failed to mark CHECKPOINTED for taskId="
                                        + task.getTaskId(), e);
                            }
                        }
                        else
                        {
                            // restoreOldFiles is derived uniformly from the catalog. For a
                            // SWAPPED task the swap definitely committed, so isSwapCommitted
                            // returns true (its newFile stays REGULAR because
                            // collectPendingFileIds protects it from orphan-retirement); for
                            // an INDEX_SWITCHING task the swap may or may not have committed,
                            // so the catalog is the source of truth.
                            rollbackTask(task, isSwapCommitted(task));
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        private void rollbackTask(Task task, boolean restoreOldFiles) throws RetinaException
        {
            restorePrimaryIndex(task);
            if (restoreOldFiles)
            {
                restoreOldFileCatalog(task);
            }
            cleanupNewFile(task);
            try { wal.markAborted(task.getTaskId()); }
            catch (IOException e)
            {
                throw new RetinaException(
                        "WAL recovery failed to mark ABORTED for taskId="
                        + task.getTaskId(), e);
            }
        }

        private void restorePrimaryIndex(Task task) throws RetinaException
        {
            List<RollbackEntry> entries = new ArrayList<>();
            for (RollbackEntry entry : task.getRollbackEntries())
            {
                if (entry.getOldRowId() >= 0)
                {
                    entries.add(entry);
                }
            }
            if (entries.isEmpty())
            {
                return;
            }
            long primaryIndexId = getPrimaryIndexId(task.getTableId());
            IndexOption indexOption = IndexOption.builder()
                    .vNodeId(task.getVirtualNodeId()).build();
            try
            {
                indexService.restorePrimaryIndexEntries(
                        task.getTableId(), primaryIndexId, entries, indexOption);
            }
            catch (IndexException | UnsupportedOperationException e)
            {
                throw new RetinaException(
                        "WAL recovery failed to restore primary index for taskId="
                        + task.getTaskId(), e);
            }
        }

        private long getPrimaryIndexId(long tableId) throws RetinaException
        {
            try
            {
                SinglePointIndex primaryIndex = metadataService.getPrimaryIndex(tableId);
                if (primaryIndex == null)
                {
                    throw new RetinaException(
                            "WAL recovery failed: primary index not found for tableId=" + tableId);
                }
                return primaryIndex.getId();
            }
            catch (MetadataException e)
            {
                throw new RetinaException(
                        "WAL recovery failed to load primary index for tableId=" + tableId, e);
            }
        }

        private void restoreOldFileCatalog(Task task) throws RetinaException
        {
            for (Long oldFileId : task.getOldFileIds())
            {
                File file = loadRequiredFile(task, oldFileId, "old file");
                if (file.getType() == File.Type.REGULAR)
                {
                    file.setCleanupAt(null);
                    updateFile(task, file, "clear cleanupAt for old file");
                    continue;
                }
                if (file.getType() != File.Type.RETIRED)
                {
                    throw new RetinaException(
                            "WAL recovery failed: old fileId=" + oldFileId
                            + " for taskId=" + task.getTaskId()
                            + " is " + file.getType() + ", expected RETIRED or REGULAR");
                }
                file.setType(File.Type.REGULAR);
                file.setCleanupAt(null);
                updateFile(task, file, "restore old file catalog");
            }
        }

        private void cleanupNewFile(Task task) throws RetinaException
        {
            if (task.getNewRowCount() > 0)
            {
                try
                {
                    indexService.deleteMainIndexRange(task.getTableId(), task.getNewFileId(),
                            task.getNewRowIdStart(), task.getNewRowCount());
                }
                catch (IndexException | UnsupportedOperationException e)
                {
                    throw new RetinaException(
                            "WAL recovery failed to delete new MainIndex range for taskId="
                            + task.getTaskId(), e);
                }
            }

            File file = loadOptionalFile(task.getNewFileId());
            if (file != null)
            {
                if (file.getType() == File.Type.RETIRED)
                {
                    throw new RetinaException(
                            "WAL recovery failed: new fileId=" + task.getNewFileId()
                            + " for taskId=" + task.getTaskId() + " is RETIRED");
                }
                try
                {
                    if (!metadataService.deleteFiles(
                            Collections.singletonList(task.getNewFileId())))
                    {
                        throw new RetinaException(
                                "deleteFiles returned false for newFileId=" + task.getNewFileId());
                    }
                }
                catch (MetadataException e)
                {
                    throw new RetinaException(
                            "WAL recovery failed to delete new file catalog for taskId="
                            + task.getTaskId(), e);
                }
            }

            String path = task.getNewFilePath();
            if (path != null && !path.trim().isEmpty())
            {
                try
                {
                    Storage fs = StorageFactory.Instance().getStorage(path);
                    if (fs.exists(path))
                    {
                        fs.delete(path, false);
                    }
                }
                catch (IOException e)
                {
                    throw new RetinaException(
                            "WAL recovery failed to delete new physical file for taskId="
                            + task.getTaskId() + ", path=" + path, e);
                }
            }
        }

        private boolean isSwapCommitted(Task task) throws RetinaException
        {
            File newFile = loadOptionalFile(task.getNewFileId());
            if (newFile != null && newFile.getType() == File.Type.REGULAR)
            {
                return true;
            }
            for (Long oldFileId : task.getOldFileIds())
            {
                File oldFile = loadOptionalFile(oldFileId);
                if (oldFile != null && oldFile.getType() == File.Type.RETIRED)
                {
                    return true;
                }
            }
            return false;
        }

        private File loadRequiredFile(Task task, long fileId, String role) throws RetinaException
        {
            File file = loadOptionalFile(fileId);
            if (file == null)
            {
                throw new RetinaException(
                        "WAL recovery failed: missing " + role
                        + " fileId=" + fileId + " for taskId=" + task.getTaskId());
            }
            return file;
        }

        private File loadOptionalFile(long fileId) throws RetinaException
        {
            try
            {
                return metadataService.getFileById(fileId);
            }
            catch (MetadataException e)
            {
                throw new RetinaException(
                        "WAL recovery failed to load fileId=" + fileId, e);
            }
        }

        private void updateFile(Task task, File file, String action) throws RetinaException
        {
            try
            {
                if (!metadataService.updateFile(file))
                {
                    throw new RetinaException(
                            action + " returned false for fileId=" + file.getId());
                }
            }
            catch (MetadataException e)
            {
                throw new RetinaException(
                        "WAL recovery failed to " + action
                        + " for taskId=" + task.getTaskId(), e);
            }
        }
    }
}
