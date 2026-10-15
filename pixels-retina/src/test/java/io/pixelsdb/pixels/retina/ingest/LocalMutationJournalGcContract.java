/*
 * Copyright 2026 PixelsDB.
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/** Filesystem and crash-boundary checks; no services or native dependencies. */
public final class LocalMutationJournalGcContract
{
    private static final long STATEMENT_ID = 1L;

    private LocalMutationJournalGcContract() {}
    interface Work { void run() throws Exception; }
    interface InDirectory { void run(Path path) throws Exception; }
    static void check(boolean condition, String message) { if (!condition) { throw new AssertionError(message); } }
    static void fails(Work work) throws Exception
    {
        try { work.run(); } catch (IOException expected) { return; }
        throw new AssertionError("Expected IOException");
    }
    static void directory(InDirectory work) throws Exception
    {
        Path dir = Files.createTempDirectory("pixels-journal-gc-");
        try { work.run(dir); }
        finally
        {
            try (Stream<Path> paths = Files.walk(dir))
            {
                Path[] ordered = paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new);
                for (Path path : ordered) { Files.deleteIfExists(path); }
            }
        }
    }
    static LocalMutationJournal open(Path path) throws IOException
    { return new LocalMutationJournal(path, 65536, 2 * 1024 * 1024, 10000); }
    static MutationBatch batch(long tx, long seq)
    {
        return new MutationBatch(new MutationStreamId(
                tx, STATEMENT_ID, 1, 73, 0, MutationStreamId.Kind.APPEND_ROWS),
                seq, 1, 1, 100, new byte[8192]);
    }
    static MutationStreamSeal seal(MutationBatch... batches)
    {
        byte[] digest = MutationStreamSeal.emptyDigest();
        long rows = 0, bytes = 0;
        for (MutationBatch batch : batches)
        {
            digest = MutationStreamSeal.extendDigest(digest, batch.getDigest());
            rows += batch.getRowCount(); bytes += batch.getPayloadBytes();
        }
        return new MutationStreamSeal(batches[0].getStreamId(), batches.length, rows, bytes, digest);
    }
    static void appendSealed(LocalMutationJournal journal, MutationBatch batch) throws IOException
    { journal.append(batch); journal.seal(seal(batch)); }

    public static void collectsPayloadButKeepsIdentityFence() throws Exception
    {
        directory(dir -> {
            MutationBatch dead = batch(1, 0), live = batch(2, 0);
            try (LocalMutationJournal journal = open(dir))
            {
                appendSealed(journal, dead); appendSealed(journal, live);
                long before = journal.getJournalBytes();
                check(journal.compactCheckpointedTransactions(Collections.singleton(1L)) > 8000, "No payload space reclaimed");
                check(journal.getJournalBytes() < before, "File did not shrink");
                check(journal.getCheckpointedTransactions().contains(1L), "Lost completion fence");
                fails(() -> journal.append(dead));
                fails(() -> journal.discardAbortedTransaction(1));
                check(Arrays.equals(journal.readSealedBatch(live.getStreamId(), 0).getDigest(), live.getDigest()), "Changed survivor");
                check(journal.compactCheckpointedTransactions(Collections.singleton(1L)) == 0, "Repeat GC should not rotate");
                check(journal.getGeneration() == 1, "Unexpected generation");
            }
            try (LocalMutationJournal journal = open(dir))
            {
                fails(() -> journal.append(dead));
                check(journal.getSeal(live.getStreamId()).get().equals(seal(live)), "Seal lost after GC restart");
                journal.removeObsoleteGenerations();
            }
            try (Stream<Path> paths = Files.list(dir))
            { check(paths.filter(p -> p.toString().endsWith(".wal")).count() == 1, "Old WAL was not removed"); }
        });
    }
    public static void abortOnlyReclamation() throws Exception
    {
        directory(dir -> {
            MutationBatch dead = batch(3, 0), live = batch(4, 0);
            try (LocalMutationJournal journal = open(dir))
            {
                journal.append(dead); journal.discardAbortedTransaction(3); appendSealed(journal, live);
                check(journal.compactCheckpointedTransactions(Collections.emptySet()) > 8000, "ABORT payload retained");
            }
            try (LocalMutationJournal journal = open(dir))
            {
                fails(() -> journal.append(dead));
                check(journal.readSealedBatch(live.getStreamId(), 0).getRowCount() == 100, "Lost live rows");
            }
        });
    }
    public static void openStreamIsPreserved() throws Exception
    {
        directory(dir -> {
            MutationBatch dead = batch(5, 0), first = batch(6, 0), next = batch(6, 1);
            try (LocalMutationJournal journal = open(dir))
            {
                appendSealed(journal, dead); journal.append(first);
                fails(() -> journal.compactCheckpointedTransactions(Collections.singleton(6L)));
                fails(() -> journal.compactCheckpointedTransactions(Collections.singleton(999L)));
                journal.compactCheckpointedTransactions(Collections.singleton(5L));
                check(!journal.getSeal(first.getStreamId()).isPresent(), "GC implicitly sealed open stream");
            }
            try (LocalMutationJournal journal = open(dir))
            {
                journal.append(first); journal.append(next); journal.seal(seal(first, next));
                check(journal.readSealedBatch(first.getStreamId(), 1).getRowCount() == 100, "Lost open stream");
            }
        });
    }
    public static void crashAtEveryPublicationBoundary() throws Exception
    {
        for (LocalMutationJournal.GcPhase phase : LocalMutationJournal.GcPhase.values())
        {
            directory(dir -> {
                MutationBatch dead = batch(10, 0), live = batch(11, 0);
                try (LocalMutationJournal journal = new LocalMutationJournal(dir, 65536, 2 * 1024 * 1024, 10000,
                        point -> { if (point == phase) { throw new IOException("crash " + point); } }))
                {
                    appendSealed(journal, dead); appendSealed(journal, live);
                    fails(() -> journal.compactCheckpointedTransactions(Collections.singleton(10L)));
                    fails(journal::sync);
                }
                try (LocalMutationJournal journal = open(dir))
                {
                    check(journal.readSealedBatch(live.getStreamId(), 0).getRowCount() == 100, "Lost survivor at " + phase);
                    boolean afterPointer = phase == LocalMutationJournal.GcPhase.AFTER_POINTER
                            || phase == LocalMutationJournal.GcPhase.BEFORE_OLD_DELETE;
                    check(journal.getCheckpointedTransactions().contains(10L) == afterPointer, "Mixed generation at " + phase);
                    if (!afterPointer) { journal.compactCheckpointedTransactions(Collections.singleton(10L)); }
                    else { journal.removeObsoleteGenerations(); }
                }
            });
        }
    }
    public static void lockSurvivesGenerationSwitch() throws Exception
    {
        directory(dir -> {
            AtomicBoolean checked = new AtomicBoolean();
            try (LocalMutationJournal journal = new LocalMutationJournal(dir, 65536, 2 * 1024 * 1024, 10000, point -> {
                if (point != LocalMutationJournal.GcPhase.AFTER_POINTER) { return; }
                try (LocalMutationJournal unexpected = open(dir))
                { unexpected.sync(); throw new AssertionError("Second owner during generation switch"); }
                catch (IOException expected) { checked.set(true); }
            }))
            {
                appendSealed(journal, batch(20, 0)); journal.compactCheckpointedTransactions(Collections.singleton(20L));
                fails(() -> { try (LocalMutationJournal other = open(dir)) { other.sync(); } });
            }
            check(checked.get(), "Missing lock test");
            try (LocalMutationJournal journal = open(dir)) { journal.sync(); }
        });
    }
    public static void corruptSelectedGenerationNeverFallsBack() throws Exception
    {
        directory(dir -> {
            try (LocalMutationJournal journal = new LocalMutationJournal(dir, 65536, 2 * 1024 * 1024, 10000,
                    point -> { if (point == LocalMutationJournal.GcPhase.AFTER_POINTER) { throw new IOException("crash"); } }))
            {
                appendSealed(journal, batch(30, 0)); appendSealed(journal, batch(31, 0));
                fails(() -> journal.compactCheckpointedTransactions(Collections.singleton(30L)));
            }
            check(Files.exists(dir.resolve("mutations.wal")), "Test needs surviving old generation");
            try (RandomAccessFile f = new RandomAccessFile(dir.resolve("mutations.1.wal").toFile(), "rw"))
            { f.setLength(8); }
            fails(() -> { try (LocalMutationJournal journal = open(dir)) { journal.sync(); } });
        });
    }
    public static void multipleCyclesAndAdmissionRecovery() throws Exception
    {
        directory(dir -> {
            try (LocalMutationJournal journal = new LocalMutationJournal(dir, 65536, 40000, 10000))
            {
                for (long tx = 40; tx < 90; tx++)
                {
                    MutationBatch batch = batch(tx, 0);
                    appendSealed(journal, batch);
                    journal.compactCheckpointedTransactions(Collections.singleton(tx));
                }
                check(journal.getCheckpointedTransactions().size() == 50, "Lost identity fences");
                check(journal.getJournalBytes() < 2000, "Retained large payloads");
            }
            try (LocalMutationJournal journal = open(dir))
            {
                check(journal.getCheckpointedTransactions().size() == 50, "Lost fences on restart");
                fails(() -> journal.append(batch(40, 0)));
                appendSealed(journal, batch(100, 0));
            }
        });
    }
    public static void differentTablesAndKindsArePreserved() throws Exception
    {
        directory(dir -> {
            MutationBatch dead = batch(200, 0);
            MutationBatch deletion = new MutationBatch(new MutationStreamId(
                    201, STATEMENT_ID, 1, 74, 1, MutationStreamId.Kind.DELETE_ROWS),
                    0, 1, 1, 1, new byte[]{4, 5});
            try (LocalMutationJournal journal = open(dir))
            {
                appendSealed(journal, dead); appendSealed(journal, deletion);
                journal.compactCheckpointedTransactions(Collections.singleton(200L));
            }
            try (LocalMutationJournal journal = open(dir))
            { check(journal.readSealedBatch(deletion.getStreamId(), 0).getStreamId().equals(deletion.getStreamId()), "Lost delete identity"); }
        });
    }
    public static void checkpointFenceSurvivesDeferredCompaction() throws Exception
    {
        directory(dir -> {
            MutationBatch first = batch(301, 0), second = batch(302, 0), third = batch(303, 0);
            try (LocalMutationJournal journal = open(dir))
            {
                appendSealed(journal, first);
                appendSealed(journal, second);
                appendSealed(journal, third);
                long before = journal.getJournalBytes();
                journal.checkpointTransaction(301);
                journal.checkpointTransaction(301);
                fails(() -> journal.append(first));
                check(journal.getJournalBytes() > before, "Checkpoint should append a fence before reclamation");
                check(journal.compactRetiredTransactions() == 0, "Rewrote more live payload than reclaimed");
                check(journal.getGeneration() == 0, "Premature WAL generation rewrite");
            }
            try (LocalMutationJournal journal = open(dir))
            {
                fails(() -> journal.readSealedBatch(first.getStreamId(), 0));
                fails(() -> journal.append(first));
                check(Arrays.equals(journal.readSealedBatch(third.getStreamId(), 0).getDigest(),
                        third.getDigest()), "Deferred GC lost a live batch");
                journal.checkpointTransaction(302);
                check(journal.compactRetiredTransactions() > first.getPayloadBytes(),
                        "Grouped checkpoint did not reclaim retired payloads");
                check(journal.getGeneration() == 1, "Checkpoint group requires one rewrite");
                journal.checkpointTransaction(303);
            }
            try (LocalMutationJournal journal = open(dir))
            {
                fails(() -> journal.append(third));
                check(journal.compactRetiredTransactions() > 0, "Recovery did not reclaim the fenced tail");
                check(journal.getCheckpointedTransactions().size() == 3, "Reclamation lost terminal fences");
            }
        });
    }

    public static void main(String[] args) throws Exception
    {
        collectsPayloadButKeepsIdentityFence(); abortOnlyReclamation(); openStreamIsPreserved();
        crashAtEveryPublicationBoundary(); lockSurvivesGenerationSwitch();
        corruptSelectedGenerationNeverFallsBack(); multipleCyclesAndAdmissionRecovery();
        differentTablesAndKindsArePreserved();
        checkpointFenceSurvivesDeferredCompaction();
        System.out.println("Journal GC contract: 13 cases passed (including 5 crash boundaries)");
    }
}
