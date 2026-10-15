/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */
package io.pixelsdb.pixels.daemon.transaction.ingest;

import io.pixelsdb.pixels.common.ingest.durable.AtomicStateFile;
import io.pixelsdb.pixels.ingest.IngestProto.CoordinatorMutation;
import io.pixelsdb.pixels.ingest.IngestProto.CoordinatorSnapshot;
import io.pixelsdb.pixels.ingest.IngestProto.TerminalTransaction;
import io.pixelsdb.pixels.ingest.IngestProto.Transaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/** Durable coordinator checkpoint with incremental transaction-state replacements. */
public final class CoordinatorStateStore implements DurableIngestCoordinator.StateStore
{
    private static final int JOURNAL_MAGIC = 0x50584344;
    private static final int JOURNAL_VERSION = 1;
    private static final int MUTATION_VERSION = 1;
    private static final int HEADER_BYTES = Integer.BYTES * 2;
    private static final int FRAME_HEADER_BYTES = Integer.BYTES * 2;

    private final Path directory;
    private final Path journalPath;
    private final AtomicStateFile checkpoint;
    private final int maximumBytes;
    private final int compactionBytes;
    private FileChannel journal;
    private CoordinatorSnapshot current;
    private boolean failed;

    public CoordinatorStateStore(Path directory, int maximumBytes, int compactionBytes)
            throws IOException
    {
        if (maximumBytes <= HEADER_BYTES || compactionBytes <= HEADER_BYTES
                || compactionBytes > maximumBytes)
        {
            throw new IllegalArgumentException("Invalid coordinator journal limits");
        }
        this.directory = directory;
        this.journalPath = directory.resolve("decisions.log");
        this.maximumBytes = maximumBytes;
        this.compactionBytes = compactionBytes;
        this.checkpoint = new AtomicStateFile(directory, maximumBytes);
        try
        {
            byte[] bytes = checkpoint.read();
            current = bytes.length == 0 ? null : CoordinatorSnapshot.parseFrom(bytes);
            openAndRecover();
        }
        catch (IOException e)
        {
            if (journal != null)
            {
                try
                {
                    journal.close();
                }
                catch (IOException closeFailure)
                {
                    e.addSuppressed(closeFailure);
                }
            }
            try
            {
                checkpoint.close();
            }
            catch (IOException closeFailure)
            {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    @Override
    public synchronized CoordinatorSnapshot read() throws IOException
    {
        ensureOpen();
        return current;
    }

    @Override
    public synchronized void store(CoordinatorSnapshot value, Durability durability)
            throws IOException
    {
        ensureOpen();
        java.util.Objects.requireNonNull(durability, "durability");
        CoordinatorMutation mutation = mutation(current, value);
        byte[] body = mutation.toByteArray();
        int frameBytes = Math.addExact(FRAME_HEADER_BYTES, body.length);
        if (journal.size() + frameBytes > maximumBytes)
        {
            compact();
        }
        if (journal.size() + frameBytes > maximumBytes)
        {
            throw new IOException("Coordinator journal capacity exceeded");
        }
        ByteBuffer frame = ByteBuffer.allocate(frameBytes)
                .putInt(body.length)
                .putInt(checksum(body))
                .put(body);
        frame.flip();
        try
        {
            writeFully(journal, frame);
            if (durability == Durability.SYNCHRONIZED)
            {
                journal.force(true);
            }
            current = value;
            if (journal.size() >= compactionBytes)
            {
                compact();
            }
        }
        catch (IOException e)
        {
            failed = true;
            throw e;
        }
    }

    @Override
    public synchronized void synchronize() throws IOException
    {
        ensureOpen();
        try
        {
            journal.force(true);
        }
        catch (IOException e)
        {
            failed = true;
            throw e;
        }
    }

    private void openAndRecover() throws IOException
    {
        Files.createDirectories(directory);
        if (!Files.exists(journalPath))
        {
            journal = FileChannel.open(journalPath, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            writeHeader(journal);
            journal.force(true);
            forceDirectory();
            return;
        }
        journal = FileChannel.open(journalPath,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (journal.size() < HEADER_BYTES)
        {
            throw new IOException("Coordinator journal header is incomplete");
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        readFully(journal, header, 0L);
        header.flip();
        if (header.getInt() != JOURNAL_MAGIC || header.getInt() != JOURNAL_VERSION)
        {
            throw new IOException("Invalid coordinator journal header");
        }
        long offset = HEADER_BYTES;
        while (offset < journal.size())
        {
            long remaining = journal.size() - offset;
            if (remaining < FRAME_HEADER_BYTES)
            {
                truncateTornTail(offset);
                break;
            }
            ByteBuffer frameHeader = ByteBuffer.allocate(FRAME_HEADER_BYTES);
            readFully(journal, frameHeader, offset);
            frameHeader.flip();
            int length = frameHeader.getInt();
            int expectedChecksum = frameHeader.getInt();
            if (length <= 0 || length > maximumBytes)
            {
                throw new IOException("Invalid coordinator journal frame length");
            }
            if (remaining - FRAME_HEADER_BYTES < length)
            {
                truncateTornTail(offset);
                break;
            }
            ByteBuffer body = ByteBuffer.allocate(length);
            readFully(journal, body, offset + FRAME_HEADER_BYTES);
            byte[] bytes = body.array();
            if (checksum(bytes) != expectedChecksum)
            {
                throw new IOException("Coordinator journal checksum mismatch");
            }
            current = apply(current, CoordinatorMutation.parseFrom(bytes));
            offset += FRAME_HEADER_BYTES + length;
        }
        journal.position(journal.size());
    }

    private static CoordinatorMutation mutation(
            CoordinatorSnapshot previous, CoordinatorSnapshot next)
    {
        Map<Long, Transaction> oldTransactions = transactions(previous);
        Map<Long, TerminalTransaction> oldTerminals = terminals(previous);
        CoordinatorMutation.Builder result = CoordinatorMutation.newBuilder()
                .setVersion(MUTATION_VERSION)
                .setSnapshotVersion(next.getVersion())
                .setPublishedTimestamp(next.getPublishedTimestamp())
                .setLastCommitTimestamp(next.getLastCommitTimestamp())
                .addAllRoutes(next.getRoutesList())
                .setRetiredTransactionIdHighWatermark(
                        next.getRetiredTransactionIdHighWatermark());
        for (Transaction transaction : next.getTransactionsList())
        {
            if (!transaction.equals(oldTransactions.remove(transaction.getTransactionId())))
            {
                result.addUpsertTransactions(transaction);
            }
        }
        result.addAllRemoveTransactionIds(oldTransactions.keySet());
        for (TerminalTransaction terminal : next.getTerminalTransactionsList())
        {
            long transactionId = terminal.getTransaction().getTransactionId();
            if (!terminal.equals(oldTerminals.remove(transactionId)))
            {
                result.addUpsertTerminalTransactions(terminal);
            }
        }
        result.addAllRemoveTerminalTransactionIds(oldTerminals.keySet());
        return result.build();
    }

    private static CoordinatorSnapshot apply(
            CoordinatorSnapshot previous, CoordinatorMutation mutation) throws IOException
    {
        if (mutation.getVersion() != MUTATION_VERSION)
        {
            throw new IOException("Unknown coordinator mutation version");
        }
        Map<Long, Transaction> transactions = transactions(previous);
        for (long transactionId : mutation.getRemoveTransactionIdsList())
        {
            transactions.remove(transactionId);
        }
        for (Transaction transaction : mutation.getUpsertTransactionsList())
        {
            transactions.put(transaction.getTransactionId(), transaction);
        }
        Map<Long, TerminalTransaction> terminals = terminals(previous);
        for (long transactionId : mutation.getRemoveTerminalTransactionIdsList())
        {
            terminals.remove(transactionId);
        }
        for (TerminalTransaction terminal : mutation.getUpsertTerminalTransactionsList())
        {
            terminals.put(terminal.getTransaction().getTransactionId(), terminal);
        }
        return CoordinatorSnapshot.newBuilder()
                .setVersion(mutation.getSnapshotVersion())
                .setPublishedTimestamp(mutation.getPublishedTimestamp())
                .setLastCommitTimestamp(mutation.getLastCommitTimestamp())
                .addAllTransactions(transactions.values())
                .addAllRoutes(mutation.getRoutesList())
                .addAllTerminalTransactions(terminals.values())
                .setRetiredTransactionIdHighWatermark(
                        mutation.getRetiredTransactionIdHighWatermark())
                .build();
    }

    private static Map<Long, Transaction> transactions(CoordinatorSnapshot snapshot)
    {
        Map<Long, Transaction> result = new LinkedHashMap<>();
        if (snapshot != null)
        {
            for (Transaction transaction : snapshot.getTransactionsList())
            {
                result.put(transaction.getTransactionId(), transaction);
            }
        }
        return result;
    }

    private static Map<Long, TerminalTransaction> terminals(CoordinatorSnapshot snapshot)
    {
        Map<Long, TerminalTransaction> result = new LinkedHashMap<>();
        if (snapshot != null)
        {
            for (TerminalTransaction terminal : snapshot.getTerminalTransactionsList())
            {
                result.put(terminal.getTransaction().getTransactionId(), terminal);
            }
        }
        return result;
    }

    private void compact() throws IOException
    {
        if (current == null)
        {
            return;
        }
        checkpoint.store(current.toByteArray());
        Path replacement = directory.resolve("decisions.log.new");
        boolean adopted = false;
        try
        {
            try (FileChannel output = FileChannel.open(replacement,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE))
            {
                writeHeader(output);
                output.force(true);
            }
            journal.close();
            Files.move(replacement, journalPath, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            adopted = true;
            forceDirectory();
            journal = FileChannel.open(journalPath,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            journal.position(journal.size());
        }
        finally
        {
            if (!adopted)
            {
                Files.deleteIfExists(replacement);
            }
        }
    }

    private void ensureOpen() throws IOException
    {
        if (failed || !journal.isOpen())
        {
            throw new IOException("Coordinator state unavailable; recovery required");
        }
    }

    private void truncateTornTail(long offset) throws IOException
    {
        journal.truncate(offset);
        journal.force(true);
    }

    private void forceDirectory() throws IOException
    {
        try (FileChannel directoryChannel = FileChannel.open(directory, StandardOpenOption.READ))
        {
            directoryChannel.force(true);
        }
    }

    private static void writeHeader(FileChannel channel) throws IOException
    {
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES)
                .putInt(JOURNAL_MAGIC)
                .putInt(JOURNAL_VERSION);
        header.flip();
        writeFully(channel, header);
    }

    private static int checksum(byte[] bytes)
    {
        CRC32 checksum = new CRC32();
        checksum.update(bytes);
        return (int) checksum.getValue();
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException
    {
        while (buffer.hasRemaining())
        {
            channel.write(buffer);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long offset)
            throws IOException
    {
        while (buffer.hasRemaining())
        {
            int read = channel.read(buffer, offset + buffer.position());
            if (read < 0)
            {
                throw new IOException("Unexpected end of coordinator journal");
            }
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        IOException failure = null;
        try
        {
            if (!failed)
            {
                journal.force(true);
            }
            journal.close();
        }
        catch (IOException e)
        {
            failure = e;
        }
        try
        {
            checkpoint.close();
        }
        catch (IOException e)
        {
            if (failure == null)
            {
                failure = e;
            }
            else
            {
                failure.addSuppressed(e);
            }
        }
        if (failure != null)
        {
            throw failure;
        }
    }
}
