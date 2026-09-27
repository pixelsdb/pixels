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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/** The same contract cases run under JUnit or directly with a JDK. */
public final class LocalMutationJournalContract
{
    private static final long STATEMENT_ID = 1L;
    private static final int GROUP_COMMIT_STREAMS = 8;
    private static final long GROUP_COMMIT_DELAY_MICROS = 50_000L;
    private static final long CACHED_BATCH_OVERHEAD_BYTES = 128L;

    private LocalMutationJournalContract() {}

    public static void roundTripAndStreamIsolation() throws Exception
    {
        withDirectory(dir -> {
            MutationBatch a = batch(id(1, 1), 0, 2, new byte[]{1, 1});
            MutationBatch b = batch(id(1, 2), 0, 1, new byte[]{2});
            MutationBatch c = batch(id(1, 2), 1, 1, new byte[]{3});
            try (LocalMutationJournal journal = open(dir))
            {
                journal.append(a);
                journal.append(b);
                expect(IOException.class, () -> journal.readSealedBatch(a.getStreamId(), 0));
                journal.seal(seal(a));
                check(!journal.getSeal(b.getStreamId()).isPresent(), "Sealed another writer's stream");
                journal.append(c);
                journal.seal(seal(b, c));
            }
            try (LocalMutationJournal journal = open(dir))
            {
                check(journal.getSeal(a.getStreamId()).get().equals(seal(a)), "Lost seal");
                check(Arrays.equals(journal.readSealedBatch(a.getStreamId(), 0).getPayload(), a.getPayload()), "Payload mismatch");
                check(journal.readSealedBatch(b.getStreamId(), 1).getPayload()[0] == 3, "Interleaved replay mismatch");
            }
        });
    }

    public static void boundedPrivateCacheFallsBackToWal() throws Exception
    {
        withDirectory(dir -> {
            MutationBatch first = batch(id(101, 1), 0, 1, new byte[]{1});
            MutationBatch second = batch(id(102, 1), 0, 1, new byte[]{2});
            long oneBatchBudget = first.getPayloadBytes() + CACHED_BATCH_OVERHEAD_BYTES;
            try (LocalMutationJournal journal = new LocalMutationJournal(
                    dir, 1024, 1_000_000, 1000, 0L, oneBatchBudget))
            {
                journal.append(first);
                expect(IOException.class, () -> journal.readSealedBatch(first.getStreamId(), 0));
                journal.seal(seal(first));
                check(journal.readSealedBatch(first.getStreamId(), 0) == first,
                        "Durable batch was needlessly reread from WAL");
                journal.append(second);
                journal.seal(seal(second));
                check(journal.getCachedBatchCount() == 1, "Cache exceeded batch budget");
                check(journal.getCachedPayloadBytes() <= oneBatchBudget, "Cache exceeded byte budget");
                check(journal.readSealedBatch(first.getStreamId(), 0) != first,
                        "Evicted batch did not fall back to WAL");
                journal.discardAbortedTransaction(101);
                expect(IOException.class, () -> journal.readSealedBatch(first.getStreamId(), 0));
            }
            try (LocalMutationJournal recovered = new LocalMutationJournal(
                    dir, 1024, 1_000_000, 1000, 0L, oneBatchBudget))
            {
                check(recovered.getCachedBatchCount() == 0, "Recovered cache was not empty");
                check(Arrays.equals(recovered.readSealedBatch(second.getStreamId(), 0).getPayload(),
                        second.getPayload()), "Recovery lost uncached WAL payload");
            }
        });
    }

    public static void duplicatesAndDefensiveCopies() throws Exception
    {
        withDirectory(dir -> {
            byte[] payload = {4, 4};
            MutationBatch a = batch(id(2, 1), 0, 2, payload);
            payload[0] = 9;
            byte[] digest = a.getDigest();
            digest[0] ^= 1;
            a.getPayload()[0] = 9;
            try (LocalMutationJournal journal = open(dir))
            {
                long offset = journal.append(a);
                check(journal.append(batch(id(2, 1), 0, 2, new byte[]{4, 4})) == offset, "Duplicate appended twice");
                expect(IOException.class, () -> journal.append(batch(id(2, 1), 0, 1, new byte[]{4, 4})));
                journal.seal(seal(a));
                check(journal.append(a) == offset, "Retransmission after seal rejected");
                check(journal.seal(seal(a)).equals(seal(a)), "Repeated seal mismatch");
            }
            try (LocalMutationJournal journal = open(dir))
            {
                check(journal.append(a) == 8, "Replay lost dedup identity");
                check(journal.readSealedBatch(a.getStreamId(), 0).getPayload()[0] == 4, "Payload was mutable");
            }
        });
    }

    public static void gapsSchemaAndSealValidation() throws Exception
    {
        withDirectory(dir -> {
            MutationStreamId id = id(3, 1);
            MutationBatch a = batch(id, 0, 1, new byte[]{1});
            MutationBatch b = batch(id, 1, 1, new byte[]{2});
            try (LocalMutationJournal journal = open(dir))
            {
                expect(IOException.class, () -> journal.append(b));
                journal.append(a);
                expect(IOException.class, () -> journal.append(batch(id, 2, 1, new byte[]{3})));
                expect(IOException.class, () -> journal.append(new MutationBatch(id, 1, 8, 1, 1, new byte[]{2})));
                expect(IOException.class, () -> journal.append(new MutationBatch(id, 1, 7, 2, 1, new byte[]{2})));
                expect(IOException.class, () -> journal.seal(seal(a, b)));
                journal.append(b);
                MutationStreamSeal good = seal(a, b);
                expect(IOException.class, () -> journal.seal(new MutationStreamSeal(id, 2, 3, 2, good.getDigest())));
                journal.seal(good);
                expect(IOException.class, () -> journal.append(batch(id, 2, 1, new byte[]{3})));
            }
        });
    }

    public static void abortFencesOldAndNewStreams() throws Exception
    {
        withDirectory(dir -> {
            MutationBatch a = batch(id(4, 1), 0, 1, new byte[]{1});
            try (LocalMutationJournal journal = open(dir))
            {
                journal.append(a);
                journal.seal(seal(a));
                journal.discardAbortedTransaction(4);
                journal.discardAbortedTransaction(4);
                journal.discardAbortedTransaction(5);
                expect(IOException.class, () -> journal.append(a));
                expect(IOException.class, () -> journal.append(batch(id(4, 99), 0, 1, new byte[]{1})));
            }
            try (LocalMutationJournal journal = open(dir))
            {
                expect(IOException.class, () -> journal.readSealedBatch(a.getStreamId(), 0));
                expect(IOException.class, () -> journal.append(batch(id(5, 1), 0, 1, new byte[]{1})));
                MutationBatch other = batch(id(6, 1), 0, 1, new byte[]{1});
                journal.append(other);
                journal.seal(seal(other));
            }
        });
    }

    public static void unacknowledgedSuffixIsDiscarded() throws Exception
    {
        withDirectory(dir -> {
            MutationBatch a = batch(id(7, 1), 0, 1, new byte[]{1});
            MutationBatch b = batch(id(8, 1), 0, 1, new byte[]{2});
            long cut;
            try (LocalMutationJournal journal = open(dir))
            {
                journal.append(a);
                journal.seal(seal(a));
                cut = journal.getDurableOffset();
                journal.append(b);
            }
            check(Files.size(dir.resolve(LocalMutationJournal.WAL_NAME)) > cut, "No suffix written");
            try (LocalMutationJournal journal = open(dir))
            {
                check(Files.size(dir.resolve(LocalMutationJournal.WAL_NAME)) == cut, "Unacknowledged suffix retained");
                check(journal.append(b) == cut, "Discarded batch did not reuse sequence");
                journal.seal(seal(b));
            }
        });
    }

    public static void acknowledgedTruncationFailsClosed() throws Exception
    {
        withDirectory(dir -> {
            prepareOne(dir);
            Path path = dir.resolve(LocalMutationJournal.WAL_NAME);
            long size = Files.size(path);
            try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) { file.setLength(size - 1); }
            expect(IOException.class, () -> { try (LocalMutationJournal ignored = open(dir)) { ignored.getDurableOffset(); } });
            check(Files.size(path) == size - 1, "Recovery silently rewrote corrupt WAL");
        });
    }

    public static void acknowledgedCorruptionFailsClosed() throws Exception
    {
        withDirectory(dir -> {
            prepareOne(dir);
            flip(dir.resolve(LocalMutationJournal.WAL_NAME), 24);
            expect(IOException.class, () -> { try (LocalMutationJournal ignored = open(dir)) { ignored.getDurableOffset(); } });
        });
    }

    public static void corruptMarkerFailsClosed() throws Exception
    {
        withDirectory(dir -> {
            prepareOne(dir);
            flip(dir.resolve(LocalMutationJournal.MARKER_NAME), 10);
            expect(IOException.class, () -> { try (LocalMutationJournal ignored = open(dir)) { ignored.getDurableOffset(); } });
        });
    }

    public static void missingWalFailsClosed() throws Exception
    {
        withDirectory(dir -> {
            prepareOne(dir);
            Path wal = dir.resolve(LocalMutationJournal.WAL_NAME);
            Files.delete(wal);
            expect(IOException.class, () -> { try (LocalMutationJournal ignored = open(dir)) { ignored.getDurableOffset(); } });
            check(!Files.exists(wal), "Missing acknowledged WAL was recreated");
        });
    }

    public static void missingMarkerFailsClosed() throws Exception
    {
        withDirectory(dir -> {
            prepareOne(dir);
            Files.delete(dir.resolve(LocalMutationJournal.MARKER_NAME));
            expect(IOException.class, () -> { try (LocalMutationJournal ignored = open(dir)) { ignored.getDurableOffset(); } });
        });
    }

    public static void exclusiveOwner() throws Exception
    {
        withDirectory(dir -> {
            try (LocalMutationJournal first = open(dir))
            {
                expect(IOException.class, () -> { try (LocalMutationJournal ignored = open(dir)) { ignored.getDurableOffset(); } });
                first.sync();
            }
            try (LocalMutationJournal next = open(dir)) { next.sync(); }
        });
    }

    public static void admissionLimits() throws Exception
    {
        withDirectory(dir -> {
            MutationBatch a = batch(id(10, 1), 0, 1, new byte[]{1});
            try (LocalMutationJournal journal = new LocalMutationJournal(dir, 2, 10000, 2))
            {
                expect(IOException.class, () -> journal.append(batch(id(10, 1), 0, 1, new byte[]{1, 2, 3})));
                journal.append(a);
                journal.seal(seal(a));
                expect(IOException.class, () -> journal.append(batch(id(11, 1), 0, 1, new byte[]{1})));
                journal.append(a);
            }
            expect(IOException.class, () -> { try (LocalMutationJournal ignored = new LocalMutationJournal(dir, 2, 10000, 1)) { ignored.getDurableOffset(); } });
        });
        withDirectory(dir -> {
            try (LocalMutationJournal journal = new LocalMutationJournal(dir, 16, 8, 10))
            {
                expect(IOException.class, () -> journal.append(batch(id(12, 1), 0, 1, new byte[]{1})));
                journal.sync();
            }
        });
    }

    public static void postOpenCorruptionPoisonsJournal() throws Exception
    {
        withDirectory(dir -> {
            MutationBatch a = batch(id(13, 1), 0, 1, new byte[]{1});
            try (LocalMutationJournal journal = open(dir))
            {
                journal.append(a);
                journal.seal(seal(a));
                flip(dir.resolve(LocalMutationJournal.WAL_NAME), 24);
                expect(IOException.class, () -> journal.readSealedBatch(a.getStreamId(), 0));
                expect(IOException.class, journal::sync);
            }
        });
    }

    public static void manyTransactionsShareOneWal() throws Exception
    {
        withDirectory(dir -> {
            try (LocalMutationJournal journal = open(dir))
            {
                for (int tx = 20; tx < 84; tx++)
                {
                    MutationBatch a = batch(id(tx, 1), 0, 1, new byte[]{7});
                    journal.append(a);
                    journal.seal(seal(a));
                }
            }
            try (Stream<Path> files = Files.list(dir)) { check(files.filter(path -> path.getFileName().toString().endsWith(".wal")).count() == 1, "Created per-SQL staging WALs"); }
            try (LocalMutationJournal journal = open(dir))
            {
                for (int tx = 20; tx < 84; tx++)
                {
                    check(journal.readSealedBatch(id(tx, 1), 0).getRowCount() == 1, "Lost equal-valued input row");
                }
            }
        });
    }

    public static void concurrentSealsShareOneDurabilityBarrier() throws Exception
    {
        withDirectory(dir -> {
            List<MutationBatch> batches = new ArrayList<>();
            try (LocalMutationJournal journal = new LocalMutationJournal(
                    dir, 1024, 1_000_000, 1000, GROUP_COMMIT_DELAY_MICROS))
            {
                for (int stream = 0; stream < GROUP_COMMIT_STREAMS; stream++)
                {
                    MutationBatch batch = batch(id(100 + stream, 1), 0, 1,
                            new byte[] {(byte) stream});
                    journal.append(batch);
                    batches.add(batch);
                }
                long before = journal.getSyncCount();
                CountDownLatch ready = new CountDownLatch(GROUP_COMMIT_STREAMS);
                CountDownLatch start = new CountDownLatch(1);
                ExecutorService executor = Executors.newFixedThreadPool(GROUP_COMMIT_STREAMS);
                List<Future<?>> seals = new ArrayList<>();
                try
                {
                    for (MutationBatch batch : batches)
                    {
                        seals.add(executor.submit(() -> {
                            ready.countDown();
                            start.await();
                            journal.seal(seal(batch));
                            return null;
                        }));
                    }
                    ready.await();
                    start.countDown();
                    for (Future<?> seal : seals)
                    {
                        seal.get();
                    }
                }
                finally
                {
                    executor.shutdownNow();
                }
                check(journal.getSyncCount() == before + 1L,
                        "Concurrent seals did not share one durability barrier");
            }
            try (LocalMutationJournal recovered = open(dir))
            {
                for (MutationBatch batch : batches)
                {
                    check(recovered.getSeal(batch.getStreamId()).isPresent(),
                            "Group-committed seal was not recovered");
                }
            }
        });
    }

    public static void syncDoesNotSeal() throws Exception
    {
        withDirectory(dir -> {
            MutationBatch a = batch(id(90, 1), 0, 1, new byte[]{1});
            try (LocalMutationJournal journal = open(dir)) { journal.append(a); journal.sync(); }
            try (LocalMutationJournal journal = open(dir))
            {
                check(!journal.getSeal(a.getStreamId()).isPresent(), "Sync incorrectly sealed stream");
                check(journal.append(a) == 8, "Synced unsealed batch lost identity");
                journal.seal(seal(a));
            }
        });
    }

    public static void mutationKindsAreIndependent() throws Exception
    {
        withDirectory(dir -> {
            MutationBatch a = batch(id(91, 1), 0, 1, new byte[]{1});
            MutationBatch d = batch(new MutationStreamId(
                    91, STATEMENT_ID, 1, 1, 0, MutationStreamId.Kind.DELETE_ROWS),
                    0, 1, new byte[]{1});
            try (LocalMutationJournal journal = open(dir))
            {
                journal.append(a); journal.append(d);
                journal.seal(seal(a)); journal.seal(seal(d));
            }
            try (LocalMutationJournal journal = open(dir))
            {
                check(journal.readSealedBatch(d.getStreamId(), 0).getStreamId().getKind() == MutationStreamId.Kind.DELETE_ROWS, "Lost operation kind");
            }
        });
    }

    public static void incompleteFrameInDurablePrefixFailsClosed() throws Exception
    {
        withDirectory(dir -> {
            prepareOne(dir);
            try (RandomAccessFile file = new RandomAccessFile(dir.resolve(LocalMutationJournal.WAL_NAME).toFile(), "rw"))
            {
                file.seek(8); file.writeInt(Integer.MAX_VALUE);
            }
            expect(IOException.class, () -> { try (LocalMutationJournal ignored = open(dir)) { ignored.getDurableOffset(); } });
        });
    }

    public static void main(String[] args) throws Exception
    {
        roundTripAndStreamIsolation(); boundedPrivateCacheFallsBackToWal();
        duplicatesAndDefensiveCopies(); gapsSchemaAndSealValidation();
        abortFencesOldAndNewStreams(); unacknowledgedSuffixIsDiscarded(); acknowledgedTruncationFailsClosed();
        acknowledgedCorruptionFailsClosed(); corruptMarkerFailsClosed(); missingWalFailsClosed();
        missingMarkerFailsClosed(); exclusiveOwner(); admissionLimits(); postOpenCorruptionPoisonsJournal();
        manyTransactionsShareOneWal(); concurrentSealsShareOneDurabilityBarrier();
        syncDoesNotSeal(); mutationKindsAreIndependent();
        incompleteFrameInDurablePrefixFailsClosed();
        System.out.println("LocalMutationJournalContract: 19 cases passed");
    }

    private static LocalMutationJournal open(Path dir) throws IOException
    {
        return new LocalMutationJournal(dir, 1024, 1_000_000, 1000);
    }

    private static MutationStreamId id(long tx, long writer)
    {
        return new MutationStreamId(
                tx, STATEMENT_ID, writer, 1, 0, MutationStreamId.Kind.APPEND_ROWS);
    }

    private static MutationBatch batch(MutationStreamId id, long sequence, int rows, byte[] payload)
    {
        return new MutationBatch(id, sequence, 7, 1, rows, payload);
    }

    private static MutationStreamSeal seal(MutationBatch... batches)
    {
        long rows = 0, bytes = 0;
        byte[] digest = MutationStreamSeal.emptyDigest();
        for (MutationBatch batch : batches)
        {
            rows += batch.getRowCount(); bytes += batch.getPayloadBytes();
            digest = MutationStreamSeal.extendDigest(digest, batch.getDigest());
        }
        return new MutationStreamSeal(batches[0].getStreamId(), batches.length, rows, bytes, digest);
    }

    private static void prepareOne(Path dir) throws IOException
    {
        MutationBatch a = batch(id(9, 1), 0, 1, new byte[]{1});
        try (LocalMutationJournal journal = open(dir)) { journal.append(a); journal.seal(seal(a)); }
    }

    private static void flip(Path path, long position) throws IOException
    {
        try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw"))
        {
            file.seek(position); int value = file.readUnsignedByte();
            file.seek(position); file.writeByte(value ^ 1);
        }
    }

    private static void withDirectory(DirectoryCase test) throws Exception
    {
        Path dir = Files.createTempDirectory("pixels-ingest-test-");
        try { test.run(dir); }
        finally
        {
            try (Stream<Path> paths = Files.walk(dir))
            {
                Path[] ordered = paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new);
                for (Path path : ordered) { Files.deleteIfExists(path); }
            }
        }
    }

    private static void check(boolean condition, String message)
    {
        if (!condition) { throw new AssertionError(message); }
    }

    private static void expect(Class<? extends Throwable> expected, Checked action) throws Exception
    {
        try { action.run(); }
        catch (Throwable error)
        {
            if (expected.isInstance(error)) { return; }
            throw new AssertionError("Expected " + expected.getName() + ", got " + error, error);
        }
        throw new AssertionError("Expected " + expected.getName());
    }

    private interface Checked { void run() throws Exception; }
    private interface DirectoryCase { void run(Path dir) throws Exception; }
}
