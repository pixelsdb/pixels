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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.retina.ingest;

import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.MutationStreamId;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

/**
 * Bounded, transaction-private staging journal shared by many streams.
 *
 * <p>Append acknowledges acceptance only. Seal forces the WAL, atomically
 * replaces a checksummed durable-prefix marker, and syncs its directory before
 * returning. Recovery validates that entire prefix and discards only its
 * unacknowledged suffix. No recovery path treats corruption as an empty log.
 *
 * <p>The configured directory must already exist on a local filesystem with
 * directory force and atomic replacement support. There is one exclusive local
 * owner. This is not replication or distributed epoch fencing.
 *
 * <p>The LOCAL implementation uses Java NIO directly because its durability
 * contract depends on an operating-system file lock, {@link FileChannel#force(boolean)},
 * atomic replacement, and directory sync. The records are transaction WAL frames,
 * not Pixels data files. A pixels-io-backed implementation should preserve these
 * primitives behind an equivalent local durable-file abstraction.
 *
 * <p>This class never updates a MemTable, index, row allocator, or transaction
 * outcome. discardAbortedTransaction must only be invoked after the caller has
 * verified an authoritative ABORT decision. An optional bounded private cache can
 * serve durable sealed batches without rereading their WAL payloads. Checkpoint-backed compaction
 * replaces the WAL using a checksummed generation pointer. Terminal transaction
 * fences survive payload reclamation; the transaction coordinator remains the
 * authority for outcomes and for when data/index/visibility checkpoints are safe.
 *
 * <p>A returned frame offset is generation-local, not a stable external LSN.
 * Callers must retain batch identities, not these physical offsets.
 */
public final class LocalMutationJournal implements Closeable
{
    static final int MAGIC = 0x50494D4A;
    static final int MARKER_MAGIC = 0x50494D44;
    static final int VERSION = 2;
    static final int HEADER_BYTES = Integer.BYTES * 2;
    private static final int FRAME_HEADER_BYTES = Integer.BYTES * 2;
    static final String WAL_NAME = "mutations.wal";
    static final String MARKER_NAME = "durable.offset";
    private static final int GENERATION_MARKER_BYTES = 28;
    private static final String LOCK_NAME = "journal.lock";
    private static final int MAX_FIXED_BODY_BYTES = 96;
    // Record kind, stream identity, sequence/schema, format/row count/payload length.
    private static final int APPEND_METADATA_BYTES = Byte.BYTES + 6 * Long.BYTES + 5 * Integer.BYTES;
    private static final long MAX_GROUP_COMMIT_DELAY_MICROS =
            TimeUnit.SECONDS.toMicros(1L);
    private static final long CACHED_BATCH_OVERHEAD_BYTES = 128L;
    private static final int APPEND = 1;
    private static final int SEAL = 2;
    private static final int ABORT = 3;
    private static final int CHECKPOINTED = 4;

    private final Path directory;
    private final Path markerPath;
    private final int maxPayloadBytes;
    private final long maxJournalBytes;
    private final int maxRecords;
    private final long groupCommitDelayNanos;
    private final long maxCachedPayloadBytes;
    private final LinkedHashMap<Entry, MutationBatch> cachedBatches =
            new LinkedHashMap<>(16, 0.75f, true);
    private long cachedPayloadBytes;
    private FileChannel channel;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private long generation;
    private final FaultInjector faults;
    private final Set<Long> checkpointedTransactions = new HashSet<>();

    enum GcPhase { BEFORE_WAL_SYNC, AFTER_WAL_SYNC, BEFORE_POINTER, AFTER_POINTER, BEFORE_OLD_DELETE }

    interface FaultInjector
    {
        void at(GcPhase phase) throws IOException;
    }
    private final Map<MutationStreamId, StreamState> streams = new HashMap<>();
    private final Set<Long> abortedTransactions = new HashSet<>();
    private long durableOffset;
    private int recordCount;
    private boolean failed;
    private boolean closed;
    private boolean syncInProgress;
    private long syncCount;

    public LocalMutationJournal(Path directory, int maxPayloadBytes,
                                long maxJournalBytes, int maxRecords) throws IOException
    {
        this(directory, maxPayloadBytes, maxJournalBytes, maxRecords, 0L);
    }

    public LocalMutationJournal(Path directory, int maxPayloadBytes,
                                long maxJournalBytes, int maxRecords,
                                long groupCommitDelayMicros) throws IOException
    {
        this(directory, maxPayloadBytes, maxJournalBytes, maxRecords,
                groupCommitDelayMicros, 0L);
    }

    public LocalMutationJournal(Path directory, int maxPayloadBytes,
                                long maxJournalBytes, int maxRecords,
                                long groupCommitDelayMicros, long maxCachedPayloadBytes) throws IOException
    {
        this(directory, maxPayloadBytes, maxJournalBytes, maxRecords,
                groupCommitDelayMicros, maxCachedPayloadBytes, phase -> {});
    }

    LocalMutationJournal(Path directory, int maxPayloadBytes,
                         long maxJournalBytes, int maxRecords, FaultInjector faults) throws IOException
    {
        this(directory, maxPayloadBytes, maxJournalBytes, maxRecords, 0L, faults);
    }

    LocalMutationJournal(Path directory, int maxPayloadBytes,
                         long maxJournalBytes, int maxRecords, long groupCommitDelayMicros,
                         FaultInjector faults) throws IOException
    {
        this(directory, maxPayloadBytes, maxJournalBytes, maxRecords,
                groupCommitDelayMicros, 0L, faults);
    }

    LocalMutationJournal(Path directory, int maxPayloadBytes,
                         long maxJournalBytes, int maxRecords, long groupCommitDelayMicros,
                         long maxCachedPayloadBytes, FaultInjector faults) throws IOException
    {
        if (maxPayloadBytes <= 0 || maxPayloadBytes > Integer.MAX_VALUE - MAX_FIXED_BODY_BYTES - 8
                || maxJournalBytes < HEADER_BYTES || maxRecords <= 0
                || groupCommitDelayMicros < 0L
                || groupCommitDelayMicros > MAX_GROUP_COMMIT_DELAY_MICROS
                || maxCachedPayloadBytes < 0L)
        {
            throw new IllegalArgumentException("Invalid journal limits");
        }
        this.directory = directory.toRealPath();
        if (!Files.isDirectory(this.directory)) { throw new IOException("Journal directory must already exist"); }
        this.markerPath = this.directory.resolve(MARKER_NAME);
        this.maxPayloadBytes = maxPayloadBytes;
        this.maxJournalBytes = maxJournalBytes;
        this.maxRecords = maxRecords;
        this.groupCommitDelayNanos = TimeUnit.MICROSECONDS.toNanos(groupCommitDelayMicros);
        this.maxCachedPayloadBytes = maxCachedPayloadBytes;
        this.faults = java.util.Objects.requireNonNull(faults, "faults");
        this.lockChannel = FileChannel.open(this.directory.resolve(LOCK_NAME),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock acquired = null;
        try
        {
            try { acquired = lockChannel.tryLock(); }
            catch (OverlappingFileLockException e) { throw new IOException("Journal already has an owner", e); }
            if (acquired == null) { throw new IOException("Journal already has an owner"); }
            // Lock the directory across generation changes, not the replaceable WAL inode.
            if (Files.exists(markerPath)) { readMarker(); }
            Path walPath = walPath(generation);
            if (Files.exists(markerPath) && !Files.isRegularFile(walPath))
            { throw new IOException("Durable marker exists but WAL is missing"); }
            this.channel = FileChannel.open(walPath, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            if (channel.size() == 0 && !Files.exists(markerPath))
            {
                // A missing pointer with generation files is not a fresh database.
                if (hasGenerationFiles()) { throw new IOException("WAL generations exist without a durable marker"); }
                writeHeader(channel);
                persistDurablePrefix();
            }
            else { recover(); }
            this.lock = acquired;
        }
        catch (IOException | RuntimeException e)
        {
            if (channel != null) { try { channel.close(); } catch (IOException ex) { e.addSuppressed(ex); } }
            if (acquired != null) { try { acquired.release(); } catch (IOException ex) { e.addSuppressed(ex); } }
            try { lockChannel.close(); } catch (IOException ex) { e.addSuppressed(ex); }
            throw e;
        }
    }

    private Path walPath(long value)
    {
        return directory.resolve(value == 0 ? WAL_NAME : "mutations." + value + ".wal");
    }

    private boolean hasGenerationFiles() throws IOException
    {
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory, "mutations.*.wal"))
        { return paths.iterator().hasNext(); }
    }

    private static void writeHeader(FileChannel output) throws IOException
    {
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).putInt(MAGIC).putInt(VERSION);
        header.flip();
        writeFully(output, header);
    }

    /** Return the original frame offset for identical retransmissions. */
    public synchronized long append(MutationBatch batch) throws IOException
    {
        ensureOpen();
        requireNotAborted(batch.getStreamId().getTransactionId());
        if (batch.getPayloadBytes() > maxPayloadBytes)
        {
            throw new IOException("Batch exceeds journal payload limit");
        }
        StreamState state = streams.get(batch.getStreamId());
        if (state != null && batch.getSequence() < state.entries.size())
        {
            Entry existing = state.entries.get((int) batch.getSequence());
            if (!Arrays.equals(existing.digest, batch.getDigest()))
            {
                throw new IOException("Conflicting content for batch " + batch.getStreamId()
                        + "/" + batch.getSequence());
            }
            return existing.offset;
        }
        validateNextBatch(state, batch);
        long offset = appendBatch(batch);
        rememberBatch(batch, offset);
        cacheBatch(streams.get(batch.getStreamId()).entries.get((int) batch.getSequence()), batch);
        return offset;
    }

    /** Seal only this stream; other writers in the transaction remain open. */
    public MutationStreamSeal seal(MutationStreamSeal expected) throws IOException
    {
        long requiredOffset;
        MutationStreamSeal result;
        synchronized (this)
        {
            ensureOpen();
            requireNotAborted(expected.getStreamId().getTransactionId());
            StreamState state = requireStream(expected.getStreamId());
            if (!state.boundary(expected.getStreamId()).equals(expected))
            {
                throw new IOException(
                        "Stream seal does not match received batch sequence, totals, or digest");
            }
            if (state.seal == null)
            {
                appendRecord(encodeSeal(expected));
                state.seal = expected;
                state.sealEndOffset = channel.position();
            }
            requiredOffset = channel.position();
            result = state.seal;
        }
        // Repeated seal also supplies a durability barrier after a retry.
        awaitDurable(requiredOffset);
        return result;
    }

    /** Explicit local group-sync barrier; does not seal or commit any stream. */
    public void sync() throws IOException
    {
        long requiredOffset;
        synchronized (this)
        {
            ensureOpen();
            requiredOffset = channel.position();
        }
        awaitDurable(requiredOffset);
    }

    public synchronized Optional<MutationStreamSeal> getSeal(MutationStreamId id) throws IOException
    {
        ensureOpen();
        requireNotAborted(id.getTransactionId());
        StreamState state = streams.get(id);
        return state == null || state.sealEndOffset > durableOffset
                ? Optional.empty() : Optional.ofNullable(state.seal);
    }

    /** Preparation-only read. Returned bytes are not public query state. */
    public synchronized MutationBatch readSealedBatch(MutationStreamId id, long sequence) throws IOException
    {
        ensureOpen();
        requireNotAborted(id.getTransactionId());
        StreamState state = requireStream(id);
        if (state.seal == null || state.sealEndOffset > durableOffset
                || sequence < 0 || sequence >= state.entries.size())
        {
            throw new IOException("Batch is not inside a sealed stream");
        }
        try
        {
            Entry entry = state.entries.get((int) sequence);
            MutationBatch cached = cachedBatches.get(entry);
            if (cached != null) { return cached; }
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(readBody(entry.offset, durableOffset)));
            if (input.readUnsignedByte() != APPEND)
            {
                throw new IOException("Expected batch record");
            }
            MutationBatch batch = decodeBatch(input);
            requireEnd(input);
            if (!batch.getStreamId().equals(id) || batch.getSequence() != sequence
                    || !Arrays.equals(batch.getDigest(), entry.digest))
            {
                throw new IOException("Batch descriptor does not match durable bytes");
            }
            cacheBatch(entry, batch);
            return batch;
        }
        catch (IOException | RuntimeException e)
        {
            failed = true;
            throw new IOException("Cannot read staged batch; journal is fail-closed", e);
        }
    }

    /**
     * Record a verified ABORT outcome and fence all streams of that transaction.
     * The caller supplies the decision; this method must not decide an outcome.
     */
    public synchronized void discardAbortedTransaction(long transactionId) throws IOException
    {
        ensureOpen();
        if (transactionId < 0)
        {
            throw new IllegalArgumentException("Negative transaction id");
        }
        if (checkpointedTransactions.contains(transactionId))
        { throw new IOException("Cannot abort a checkpoint-covered committed transaction"); }
        if (!abortedTransactions.contains(transactionId))
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte(ABORT);
            out.writeLong(transactionId);
            appendRecord(bytes.toByteArray());
            abortedTransactions.add(transactionId);
        }
        evictTransaction(transactionId);
        persistDurablePrefix();
    }

    public synchronized long getDurableOffset() throws IOException
    {
        ensureOpen();
        return durableOffset;
    }

    private void readMarker() throws IOException
    {
        long size = Files.exists(markerPath) ? Files.size(markerPath) : -1;
        if (size != GENERATION_MARKER_BYTES)
        { throw new IOException("Missing or malformed durable-prefix marker"); }
        byte[] bytes = Files.readAllBytes(markerPath);
        ByteBuffer marker = ByteBuffer.wrap(bytes);
        if (marker.getInt() != MARKER_MAGIC) { throw new IOException("Invalid durable-prefix marker magic"); }
        int version = marker.getInt();
        if (version != VERSION) { throw new IOException("Unsupported durable-prefix marker"); }
        generation = marker.getLong();
        durableOffset = marker.getLong();
        if (marker.getInt() != checksum(bytes, 0, bytes.length - 4)
                || generation < 0 || durableOffset < HEADER_BYTES || durableOffset > maxJournalBytes)
        { throw new IOException("Invalid durable-prefix marker checksum or bounds"); }
    }

    private void recover() throws IOException
    {
        readMarker();
        if (durableOffset > channel.size())
        { throw new IOException("Truncated acknowledged WAL"); }
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        readFully(channel, header, 0);
        header.flip();
        if (header.getInt() != MAGIC || header.getInt() != VERSION)
        {
            throw new IOException("Unsupported WAL header");
        }
        long offset = HEADER_BYTES;
        try
        {
            while (offset < durableOffset)
            {
                byte[] body = readBody(offset, durableOffset);
                if (++recordCount > maxRecords)
                {
                    throw new IOException("Durable journal exceeds configured record limit");
                }
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
                int type = in.readUnsignedByte();
                if (type == APPEND)
                {
                    MutationBatch batch = decodeBatch(in);
                    requireNotAborted(batch.getStreamId().getTransactionId());
                    validateNextBatch(streams.get(batch.getStreamId()), batch);
                    rememberBatch(batch, offset);
                }
                else if (type == SEAL)
                {
                    MutationStreamSeal seal = decodeSeal(in);
                    requireNotAborted(seal.getStreamId().getTransactionId());
                    StreamState state = requireStream(seal.getStreamId());
                    if (state.seal != null || !state.boundary(seal.getStreamId()).equals(seal))
                    {
                        throw new IOException("Invalid durable stream seal");
                    }
                    state.seal = seal;
                    state.sealEndOffset = offset + FRAME_HEADER_BYTES + body.length;
                }
                else if (type == CHECKPOINTED)
                {
                    long txId = in.readLong();
                    if (txId < 0 || abortedTransactions.contains(txId) || !checkpointedTransactions.add(txId))
                    { throw new IOException("Invalid checkpointed transaction fence"); }
                }
                else if (type == ABORT)
                {
                    long txId = in.readLong();
                    if (txId < 0 || checkpointedTransactions.contains(txId) || !abortedTransactions.add(txId))
                    {
                        throw new IOException("Invalid duplicate ABORT record");
                    }
                }
                else
                {
                    throw new IOException("Unknown WAL record type: " + type);
                }
                requireEnd(in);
                offset += FRAME_HEADER_BYTES + body.length;
            }
        }
        catch (IllegalArgumentException | ArithmeticException e)
        {
            throw new IOException("Invalid durable WAL record", e);
        }
        // Only bytes not covered by an acknowledged sync may be discarded.
        if (channel.size() != durableOffset)
        {
            channel.truncate(durableOffset);
            channel.force(true);
        }
        channel.position(durableOffset);
    }

    private void persistDurablePrefix() throws IOException
    {
        try
        {
            long end = channel.position();
            channel.force(true);
            storeMarker(generation, end);
            durableOffset = end;
            syncCount++;
        }
        catch (IOException e) { failed = true; throw e; }
    }

    private void awaitDurable(long requiredOffset) throws IOException
    {
        boolean interrupted = false;
        try
        {
            synchronized (this)
            {
                while (durableOffset < requiredOffset)
                {
                    ensureOpen();
                    if (!syncInProgress)
                    {
                        syncInProgress = true;
                        try
                        {
                            if (groupCommitDelayNanos > 0L)
                            {
                                long millis = TimeUnit.NANOSECONDS.toMillis(groupCommitDelayNanos);
                                int nanos = (int) (groupCommitDelayNanos
                                        - TimeUnit.MILLISECONDS.toNanos(millis));
                                try
                                {
                                    wait(millis, nanos);
                                }
                                catch (InterruptedException ignored)
                                {
                                    interrupted = true;
                                }
                            }
                            persistDurablePrefix();
                        }
                        finally
                        {
                            syncInProgress = false;
                            notifyAll();
                        }
                    }
                    else
                    {
                        try
                        {
                            wait();
                        }
                        catch (InterruptedException ignored)
                        {
                            interrupted = true;
                        }
                    }
                }
            }
        }
        finally
        {
            if (interrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    synchronized long getSyncCount()
    {
        return syncCount;
    }

    private void storeMarker(long targetGeneration, long end) throws IOException
    {
        ByteBuffer marker = ByteBuffer.allocate(GENERATION_MARKER_BYTES);
        marker.putInt(MARKER_MAGIC)
                .putInt(VERSION)
                .putLong(targetGeneration)
                .putLong(end)
                .putInt(checksum(marker.array(), 0, GENERATION_MARKER_BYTES - Integer.BYTES))
                .flip();
        Path temporary = directory.resolve("durable.offset.tmp");
        try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
        { writeFully(output, marker); output.force(true); }
        Files.move(temporary, markerPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        forceDirectory();
    }

    private void forceDirectory() throws IOException
    {
        try (FileChannel dir = FileChannel.open(directory, StandardOpenOption.READ)) { dir.force(true); }
    }

    /**
     * Reclaim payloads for durable ABORTs and caller-verified checkpoint-covered
     * committed transactions. The caller MUST publish a recovery checkpoint covering
     * data, MainIndex, SinglePointIndex, visibility, and installation identities before
     * supplying an id here. File close, PUBLISHED alone, age, or WAL offset are not proof.
     *
     * <p>Other transactions (including unsealed, PREPARED and COMMIT_DECIDED) remain
     * byte-for-byte recoverable. This operation intentionally fences retired ids;
     * a late request is rejected rather than acknowledged as a new mutation.
     *
     * <p>Compaction blocks append/seal while copying and needs temporary disk space for a
     * second WAL. The only commit point is the generation-pointer replacement.
     * After any I/O failure the instance is fail-closed; reopen to resolve the pointer.
     *
     * @return reclaimed bytes in the active WAL, not including tiny retained fences
     */
    public synchronized long compactCheckpointedTransactions(Set<Long> coveredTransactions) throws IOException
    {
        ensureOpen();
        Set<Long> covered = new TreeSet<>(java.util.Objects.requireNonNull(coveredTransactions, "coveredTransactions"));
        for (Long id : covered)
        {
            if (id == null || id < 0 || abortedTransactions.contains(id))
            { throw new IOException("Invalid checkpoint transaction id"); }
            boolean found = checkpointedTransactions.contains(id);
            for (Map.Entry<MutationStreamId, StreamState> entry : streams.entrySet())
            {
                if (entry.getKey().getTransactionId() != id) { continue; }
                found = true;
                if (entry.getValue().seal == null) { throw new IOException("Cannot checkpoint an unsealed stream"); }
            }
            if (!found) { throw new IOException("Checkpoint references an unknown transaction"); }
        }
        // Duplicate invocations are permitted, but do not roll a file per invocation.
        boolean hasGarbage = false;
        for (MutationStreamId id : streams.keySet())
        { if (covered.contains(id.getTransactionId()) || checkpointedTransactions.contains(id.getTransactionId())
                || abortedTransactions.contains(id.getTransactionId())) { hasGarbage = true; } }
        if (!hasGarbage) { return 0; }
        FileChannel replacement = null;
        try
        {
            persistDurablePrefix(); // Retain also accepted, not-yet-sealed live batches.
            long oldSize = channel.size();
            long nextGeneration = Math.addExact(generation, 1L);
            while (Files.exists(walPath(nextGeneration))) { nextGeneration = Math.addExact(nextGeneration, 1L); }
            replacement = FileChannel.open(walPath(nextGeneration), StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            writeHeader(replacement);
            Set<Long> checkpointFences = new TreeSet<>(checkpointedTransactions);
            checkpointFences.addAll(covered);
            int keptRecords = 0;
            for (Long id : new TreeSet<>(abortedTransactions))
            { writeFrame(replacement, encodeTerminal(ABORT, id)); keptRecords++; }
            for (Long id : checkpointFences)
            { writeFrame(replacement, encodeTerminal(CHECKPOINTED, id)); keptRecords++; }
            long offset = HEADER_BYTES;
            while (offset < durableOffset)
            {
                byte[] body = readBody(offset, durableOffset); // Verify even discarded acknowledged bytes.
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
                int type = in.readUnsignedByte();
                if (type == APPEND || type == SEAL)
                {
                    long txId = readId(in).getTransactionId();
                    if (!abortedTransactions.contains(txId) && !checkpointFences.contains(txId))
                    { writeFrame(replacement, body); keptRecords++; }
                }
                else if (type != ABORT && type != CHECKPOINTED) { throw new IOException("Unknown WAL record during GC"); }
                offset += FRAME_HEADER_BYTES + body.length;
            }
            if (keptRecords > maxRecords || replacement.position() > maxJournalBytes)
            { throw new IOException("Compacted journal exceeds configured limits"); }
            faults.at(GcPhase.BEFORE_WAL_SYNC);
            replacement.force(true);
            forceDirectory(); // New filename is durable before the pointer may refer to it.
            faults.at(GcPhase.AFTER_WAL_SYNC);
            faults.at(GcPhase.BEFORE_POINTER);
            storeMarker(nextGeneration, replacement.position());
            faults.at(GcPhase.AFTER_POINTER);
            FileChannel old = channel;
            channel = replacement;
            replacement = null;
            generation = nextGeneration;
            old.close();
            clearCachedBatches();
            streams.clear(); abortedTransactions.clear(); checkpointedTransactions.clear(); recordCount = 0;
            recover(); // Rebuild generation-local offsets from the selected durable bytes.
            faults.at(GcPhase.BEFORE_OLD_DELETE);
            removeObsoleteGenerations();
            return Math.max(0L, oldSize - channel.size());
        }
        catch (IOException | RuntimeException e)
        {
            failed = true;
            throw e;
        }
        finally { if (replacement != null) { replacement.close(); } }
    }

    private static byte[] encodeTerminal(int kind, long id) throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte(kind); out.writeLong(id);
        return bytes.toByteArray();
    }

    private static void writeFrame(FileChannel output, byte[] body) throws IOException
    {
        ByteBuffer frame = ByteBuffer.allocate(8 + body.length);
        frame.putInt(body.length).putInt(checksum(body, 0, body.length)).put(body).flip();
        writeFully(output, frame);
    }

    /** Retry physical removal after a crash or transient filesystem failure. */
    public synchronized void removeObsoleteGenerations() throws IOException
    {
        ensureOpen();
        if (generation == 0) { return; }
        // A restart can observe an atomic rename before its directory sync completed.
        // Make the selected pointer durable before removing any fallback generation.
        forceDirectory();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory, "mutations*.wal"))
        {
            for (Path path : paths)
            {
                String name = path.getFileName().toString();
                if (!name.equals(WAL_NAME) && !name.matches("mutations\\.[0-9]+\\.wal")) { continue; }
                if (!path.equals(walPath(generation))) { Files.deleteIfExists(path); }
            }
        }
        forceDirectory();
    }

    public synchronized Set<Long> getCheckpointedTransactions() throws IOException
    { ensureOpen(); return Collections.unmodifiableSet(new HashSet<>(checkpointedTransactions)); }

    /**
     * Persist recovery ownership transfer without rewriting unrelated live payloads.
     * The caller must first durably checkpoint data, indexes, visibility and installation
     * identities, under the same coverage contract as compactCheckpointedTransactions.
     */
    public synchronized void checkpointTransaction(long transactionId) throws IOException
    {
        ensureOpen();
        if (checkpointedTransactions.contains(transactionId)) { return; }
        requireNotAborted(transactionId);
        boolean found = false;
        for (Map.Entry<MutationStreamId, StreamState> stream : streams.entrySet())
        {
            if (stream.getKey().getTransactionId() != transactionId) { continue; }
            found = true;
            if (stream.getValue().seal == null || stream.getValue().sealEndOffset > durableOffset)
            { throw new IOException("Cannot checkpoint an unsealed or non-durable stream"); }
        }
        if (!found) { throw new IOException("Checkpoint references an unknown transaction"); }
        appendRecord(encodeTerminal(CHECKPOINTED, transactionId));
        persistDurablePrefix();
        checkpointedTransactions.add(transactionId);
        evictTransaction(transactionId);
    }

    /** Amortize generation rewriting: copy at most as much payload as is reclaimed. */
    public synchronized long compactRetiredTransactions() throws IOException
    {
        ensureOpen();
        long retiredBytes = 0L;
        long liveBytes = 0L;
        for (Map.Entry<MutationStreamId, StreamState> stream : streams.entrySet())
        {
            long tx = stream.getKey().getTransactionId();
            if (checkpointedTransactions.contains(tx) || abortedTransactions.contains(tx))
            { retiredBytes = Math.addExact(retiredBytes, stream.getValue().bytes); }
            else
            { liveBytes = Math.addExact(liveBytes, stream.getValue().bytes); }
        }
        if (retiredBytes == 0L || retiredBytes < liveBytes) { return 0L; }
        return compactCheckpointedTransactions(Collections.emptySet());
    }

    public synchronized long getGeneration() throws IOException { ensureOpen(); return generation; }
    public synchronized long getJournalBytes() throws IOException { ensureOpen(); return channel.size(); }

    synchronized long getCachedPayloadBytes() { return cachedPayloadBytes; }
    synchronized int getCachedBatchCount() { return cachedBatches.size(); }

    private long appendRecord(byte[] body) throws IOException
    {
        ByteBuffer frame = allocateFrame(body.length);
        frame.put(body);
        return appendFrame(frame);
    }

    /** Reserve the header and reject capacity exhaustion before allocating payload storage. */
    private ByteBuffer allocateFrame(int bodyLength) throws IOException
    {
        if (recordCount >= maxRecords
                || channel.position() > maxJournalBytes - FRAME_HEADER_BYTES - bodyLength)
        {
            throw new IOException("Journal capacity exceeded; checkpoint/reclamation is required");
        }
        ByteBuffer frame = ByteBuffer.allocate(Math.addExact(bodyLength, FRAME_HEADER_BYTES));
        frame.position(FRAME_HEADER_BYTES);
        return frame;
    }

    private long appendFrame(ByteBuffer frame) throws IOException
    {
        long offset = channel.position();
        int bodyLength = frame.position() - FRAME_HEADER_BYTES;
        frame.putInt(0, bodyLength);
        frame.putInt(Integer.BYTES, checksum(frame.array(), FRAME_HEADER_BYTES, bodyLength));
        frame.flip();
        try
        {
            writeFully(channel, frame);
            recordCount++;
            return offset;
        }
        catch (IOException e)
        {
            failed = true;
            throw e;
        }
    }

    private byte[] readBody(long offset, long boundary) throws IOException
    {
        if (boundary - offset < 8)
        {
            throw new IOException("Truncated acknowledged frame header");
        }
        ByteBuffer header = ByteBuffer.allocate(FRAME_HEADER_BYTES);
        readFully(channel, header, offset);
        header.flip();
        int length = header.getInt();
        int expectedChecksum = header.getInt();
        if (length <= 0 || length > maxPayloadBytes + MAX_FIXED_BODY_BYTES
                || length > boundary - offset - 8)
        {
            throw new IOException("Invalid or truncated acknowledged frame length");
        }
        byte[] body = new byte[length];
        readFully(channel, ByteBuffer.wrap(body), offset + FRAME_HEADER_BYTES);
        if (checksum(body, 0, body.length) != expectedChecksum)
        {
            throw new IOException("Checksum mismatch in acknowledged WAL frame");
        }
        return body;
    }

    private long appendBatch(MutationBatch batch) throws IOException
    {
        ByteBuffer frame = allocateFrame(Math.addExact(APPEND_METADATA_BYTES, batch.getPayloadBytes()));
        MutationStreamId id = batch.getStreamId();
        frame.put((byte) APPEND)
                .putLong(id.getTransactionId()).putLong(id.getStatementId())
                .putLong(id.getWriterId()).putLong(id.getTableId())
                .putInt(id.getShardId()).putInt(id.getKind().getCode())
                .putLong(batch.getSequence()).putLong(batch.getSchemaVersion())
                .putInt(batch.getPayloadFormat()).putInt(batch.getRowCount())
                .putInt(batch.getPayloadBytes());
        // Copy immutable input directly into its final WAL frame, without a body staging array.
        batch.getPayloadByteString().copyTo(frame);
        return appendFrame(frame);
    }

    private MutationBatch decodeBatch(DataInputStream in) throws IOException
    {
        MutationStreamId id = readId(in);
        long sequence = in.readLong();
        long schema = in.readLong();
        int format = in.readInt();
        int rows = in.readInt();
        int length = in.readInt();
        if (length <= 0 || length > maxPayloadBytes || length != in.available())
        {
            throw new IOException("Invalid batch payload length");
        }
        byte[] payload = new byte[length];
        in.readFully(payload);
        return new MutationBatch(id, sequence, schema, format, rows, payload);
    }

    private static byte[] encodeSeal(MutationStreamSeal seal) throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeByte(SEAL);
        writeId(out, seal.getStreamId());
        out.writeLong(seal.getBatchCount());
        out.writeLong(seal.getRowCount());
        out.writeLong(seal.getPayloadBytes());
        out.write(seal.getDigest());
        return bytes.toByteArray();
    }

    private static MutationStreamSeal decodeSeal(DataInputStream in) throws IOException
    {
        MutationStreamId id = readId(in);
        long batches = in.readLong();
        long rows = in.readLong();
        long bytes = in.readLong();
        byte[] digest = new byte[MutationBatch.DIGEST_BYTES];
        in.readFully(digest);
        return new MutationStreamSeal(id, batches, rows, bytes, digest);
    }

    private static void writeId(DataOutputStream out, MutationStreamId id) throws IOException
    {
        out.writeLong(id.getTransactionId());
        out.writeLong(id.getStatementId());
        out.writeLong(id.getWriterId());
        out.writeLong(id.getTableId());
        out.writeInt(id.getShardId());
        out.writeInt(id.getKind().getCode());
    }

    private static MutationStreamId readId(DataInputStream in) throws IOException
    {
        return new MutationStreamId(in.readLong(), in.readLong(), in.readLong(), in.readLong(),
                in.readInt(), MutationStreamId.Kind.fromCode(in.readInt()));
    }

    private static void requireEnd(DataInputStream in) throws IOException
    {
        if (in.available() != 0) { throw new IOException("Trailing bytes in WAL record"); }
    }

    private void validateNextBatch(StreamState state, MutationBatch batch) throws IOException
    {
        if (batch.getSequence() != (state == null ? 0 : state.entries.size()))
        {
            throw new IOException("Mutation stream sequence has a gap or duplicate");
        }
        if (state != null && (state.seal != null || state.schema != batch.getSchemaVersion()
                || state.format != batch.getPayloadFormat()))
        {
            throw new IOException("Stream is sealed or its pinned schema/format changed");
        }
    }

    private void rememberBatch(MutationBatch batch, long offset)
    {
        StreamState state = streams.computeIfAbsent(batch.getStreamId(),
                ignored -> new StreamState(batch.getSchemaVersion(), batch.getPayloadFormat()));
        state.entries.add(new Entry(offset, batch.getDigest()));
        state.rows = Math.addExact(state.rows, batch.getRowCount());
        state.bytes = Math.addExact(state.bytes, batch.getPayloadBytes());
        state.digest = MutationStreamSeal.extendDigest(state.digest, batch.getDigest());
    }

    private void cacheBatch(Entry entry, MutationBatch batch)
    {
        long weight = (long) batch.getPayloadBytes() + CACHED_BATCH_OVERHEAD_BYTES;
        if (weight > maxCachedPayloadBytes) { return; }
        MutationBatch previous = cachedBatches.put(entry, batch);
        if (previous != null)
        { cachedPayloadBytes -= (long) previous.getPayloadBytes() + CACHED_BATCH_OVERHEAD_BYTES; }
        cachedPayloadBytes += weight;
        Iterator<Map.Entry<Entry, MutationBatch>> iterator = cachedBatches.entrySet().iterator();
        while (cachedPayloadBytes > maxCachedPayloadBytes && iterator.hasNext())
        {
            MutationBatch eldest = iterator.next().getValue();
            cachedPayloadBytes -= (long) eldest.getPayloadBytes() + CACHED_BATCH_OVERHEAD_BYTES;
            iterator.remove();
        }
    }

    private void evictTransaction(long transactionId)
    {
        Iterator<Map.Entry<Entry, MutationBatch>> iterator = cachedBatches.entrySet().iterator();
        while (iterator.hasNext())
        {
            MutationBatch batch = iterator.next().getValue();
            if (batch.getStreamId().getTransactionId() != transactionId) { continue; }
            cachedPayloadBytes -= (long) batch.getPayloadBytes() + CACHED_BATCH_OVERHEAD_BYTES;
            iterator.remove();
        }
    }

    private void clearCachedBatches()
    {
        cachedBatches.clear();
        cachedPayloadBytes = 0L;
    }

    private StreamState requireStream(MutationStreamId id) throws IOException
    {
        StreamState state = streams.get(id);
        if (state == null) { throw new IOException("Unknown mutation stream: " + id); }
        return state;
    }

    private void requireNotAborted(long txId) throws IOException
    {
        if (abortedTransactions.contains(txId)) { throw new IOException("Transaction was aborted: " + txId); }
        if (checkpointedTransactions.contains(txId)) { throw new IOException("Transaction is checkpoint-covered: " + txId); }
    }

    /** Checks terminal fences even when a transaction has no mutation streams. */
    synchronized void requireActiveTransaction(long txId) throws IOException
    {
        ensureOpen();
        requireNotAborted(txId);
    }

    private void ensureOpen() throws IOException
    {
        if (closed || failed) { throw new IOException("Journal is closed or failed; reopen for recovery"); }
    }

    private static int checksum(byte[] data, int offset, int length)
    {
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    private static void writeFully(FileChannel output, ByteBuffer data) throws IOException
    {
        while (data.hasRemaining()) { output.write(data); }
    }

    private static void readFully(FileChannel input, ByteBuffer data, long offset) throws IOException
    {
        while (data.hasRemaining())
        {
            int read = input.read(data, offset);
            if (read < 0) { throw new IOException("Unexpected end of WAL"); }
            offset += read;
        }
    }

    /** Close does not acknowledge or checkpoint unsealed appends. */
    @Override
    public synchronized void close() throws IOException
    {
        if (closed) { return; }
        boolean interrupted = false;
        while (syncInProgress)
        {
            try
            {
                wait();
            }
            catch (InterruptedException ignored)
            {
                interrupted = true;
            }
        }
        closed = true;
        clearCachedBatches();
        try
        {
            try { channel.close(); }
            finally
            {
                try { lock.release(); }
                finally { lockChannel.close(); }
            }
        }
        finally
        {
            if (interrupted) { Thread.currentThread().interrupt(); }
        }
    }

    private static final class Entry
    {
        final long offset;
        final byte[] digest;
        Entry(long offset, byte[] digest) { this.offset = offset; this.digest = digest; }
    }

    private static final class StreamState
    {
        final long schema;
        final int format;
        final List<Entry> entries = new ArrayList<>();
        long rows;
        long bytes;
        byte[] digest = MutationStreamSeal.emptyDigest();
        MutationStreamSeal seal;
        long sealEndOffset;

        StreamState(long schema, int format) { this.schema = schema; this.format = format; }

        MutationStreamSeal boundary(MutationStreamId id)
        {
            return new MutationStreamSeal(id, entries.size(), rows, bytes, digest);
        }
    }
}
