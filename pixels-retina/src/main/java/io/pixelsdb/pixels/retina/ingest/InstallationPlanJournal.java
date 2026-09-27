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
package io.pixelsdb.pixels.retina.ingest;

import io.pixelsdb.pixels.ingest.IngestProto.BatchInstall;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/** Append-only durable replacements for per-batch installation plans. */
final class InstallationPlanJournal implements Closeable
{
    private static final int MAGIC = 0x50584950;
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = Integer.BYTES * 2;
    private static final int FRAME_HEADER_BYTES = Integer.BYTES * 2;
    private static final byte UPSERT = 1;
    private static final byte REMOVE_TRANSACTION = 2;

    private final Path directory;
    private final Path path;
    private final int maximumBytes;
    private final int compactionBytes;
    private final Map<String, BatchInstall> plans = new LinkedHashMap<>();
    private FileChannel channel;
    private boolean failed;

    InstallationPlanJournal(Path directory, int maximumBytes, int compactionBytes)
            throws IOException
    {
        if (maximumBytes <= HEADER_BYTES || compactionBytes <= HEADER_BYTES
                || compactionBytes > maximumBytes)
        {
            throw new IllegalArgumentException("Invalid installation-plan journal limits");
        }
        this.directory = directory;
        this.path = directory.resolve("plans.log");
        this.maximumBytes = maximumBytes;
        this.compactionBytes = compactionBytes;
        Files.createDirectories(directory);
        openAndRecover();
    }

    synchronized Map<String, BatchInstall> plans()
    {
        return Collections.unmodifiableMap(new LinkedHashMap<>(plans));
    }

    synchronized void put(BatchInstall plan) throws IOException
    {
        ensureOpen();
        byte[] payload = plan.toByteArray();
        ByteBuffer body = ByteBuffer.allocate(Byte.BYTES + payload.length)
                .put(UPSERT)
                .put(payload);
        body.flip();
        append(body);
        plans.put(PixelsIngestInstaller.key(plan), plan);
    }

    synchronized void removeTransaction(long transactionId) throws IOException
    {
        ensureOpen();
        ByteBuffer body = ByteBuffer.allocate(Byte.BYTES + Long.BYTES)
                .put(REMOVE_TRANSACTION)
                .putLong(transactionId);
        body.flip();
        append(body);
        plans.values().removeIf(plan ->
                plan.getStream().getTransactionId() == transactionId);
        compactIfNeeded();
    }

    synchronized long size()
    {
        try
        {
            return channel.size();
        }
        catch (IOException e)
        {
            return maximumBytes;
        }
    }

    private void openAndRecover() throws IOException
    {
        if (!Files.exists(path))
        {
            channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            writeHeader(channel);
            channel.force(true);
            forceDirectory();
            return;
        }
        channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (channel.size() < HEADER_BYTES)
        {
            throw new IOException("Installation-plan journal header is incomplete");
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        readFully(channel, header, 0);
        header.flip();
        if (header.getInt() != MAGIC || header.getInt() != VERSION)
        {
            throw new IOException("Invalid installation-plan journal header");
        }
        long offset = HEADER_BYTES;
        while (offset < channel.size())
        {
            long remaining = channel.size() - offset;
            if (remaining < FRAME_HEADER_BYTES)
            {
                truncateTornTail(offset);
                break;
            }
            ByteBuffer frameHeader = ByteBuffer.allocate(FRAME_HEADER_BYTES);
            readFully(channel, frameHeader, offset);
            frameHeader.flip();
            int length = frameHeader.getInt();
            int checksum = frameHeader.getInt();
            if (length <= 0 || length > maximumBytes)
            {
                throw new IOException("Invalid installation-plan journal frame length");
            }
            if (remaining - FRAME_HEADER_BYTES < length)
            {
                truncateTornTail(offset);
                break;
            }
            ByteBuffer body = ByteBuffer.allocate(length);
            readFully(channel, body, offset + FRAME_HEADER_BYTES);
            byte[] bytes = body.array();
            if (checksum(bytes) != checksum)
            {
                throw new IOException("Installation-plan journal checksum mismatch");
            }
            apply(ByteBuffer.wrap(bytes));
            offset += FRAME_HEADER_BYTES + length;
        }
        channel.position(channel.size());
    }

    private void apply(ByteBuffer body) throws IOException
    {
        byte operation = body.get();
        if (operation == UPSERT)
        {
            byte[] payload = new byte[body.remaining()];
            body.get(payload);
            BatchInstall plan = BatchInstall.parseFrom(payload);
            plans.put(PixelsIngestInstaller.key(plan), plan);
            return;
        }
        if (operation == REMOVE_TRANSACTION && body.remaining() == Long.BYTES)
        {
            long transactionId = body.getLong();
            plans.values().removeIf(plan ->
                    plan.getStream().getTransactionId() == transactionId);
            return;
        }
        throw new IOException("Unknown installation-plan journal operation");
    }

    private void append(ByteBuffer body) throws IOException
    {
        int frameBytes = FRAME_HEADER_BYTES + body.remaining();
        if (channel.size() + frameBytes > maximumBytes)
        {
            compact();
        }
        if (channel.size() + frameBytes > maximumBytes)
        {
            throw new IOException("Installation-plan journal capacity exceeded");
        }
        byte[] bytes = new byte[body.remaining()];
        body.get(bytes);
        ByteBuffer frame = ByteBuffer.allocate(frameBytes)
                .putInt(bytes.length)
                .putInt(checksum(bytes))
                .put(bytes);
        frame.flip();
        try
        {
            writeFully(channel, frame);
            channel.force(true);
        }
        catch (IOException e)
        {
            failed = true;
            throw e;
        }
    }

    private void compactIfNeeded() throws IOException
    {
        if (channel.size() >= compactionBytes)
        {
            compact();
        }
    }

    private void compact() throws IOException
    {
        Path replacement = directory.resolve("plans.log.new");
        boolean adopted = false;
        try
        {
            try (FileChannel output = FileChannel.open(replacement,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE))
            {
                writeHeader(output);
                for (BatchInstall plan : plans.values())
                {
                    byte[] payload = plan.toByteArray();
                    byte[] body = ByteBuffer.allocate(Byte.BYTES + payload.length)
                            .put(UPSERT).put(payload).array();
                    ByteBuffer frame = ByteBuffer.allocate(FRAME_HEADER_BYTES + body.length)
                            .putInt(body.length).putInt(checksum(body)).put(body);
                    frame.flip();
                    if (output.position() + frame.remaining() > maximumBytes)
                    {
                        throw new IOException("Live installation plans exceed journal capacity");
                    }
                    writeFully(output, frame);
                }
                output.force(true);
            }
            channel.close();
            Files.move(replacement, path, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            adopted = true;
            forceDirectory();
            channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            channel.position(channel.size());
        }
        finally
        {
            if (!adopted)
            {
                Files.deleteIfExists(replacement);
            }
        }
    }

    private void truncateTornTail(long offset) throws IOException
    {
        channel.truncate(offset);
        channel.force(true);
    }

    private void forceDirectory() throws IOException
    {
        try (FileChannel directoryChannel = FileChannel.open(directory, StandardOpenOption.READ))
        {
            directoryChannel.force(true);
        }
    }

    private static int checksum(byte[] bytes)
    {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    private static void writeHeader(FileChannel output) throws IOException
    {
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).putInt(MAGIC).putInt(VERSION);
        header.flip();
        writeFully(output, header);
    }

    private static void writeFully(FileChannel output, ByteBuffer data) throws IOException
    {
        while (data.hasRemaining())
        {
            output.write(data);
        }
    }

    private static void readFully(FileChannel input, ByteBuffer data, long offset)
            throws IOException
    {
        while (data.hasRemaining())
        {
            int read = input.read(data, offset + data.position());
            if (read < 0)
            {
                throw new EOFException("Unexpected end of installation-plan journal");
            }
        }
    }

    private void ensureOpen() throws IOException
    {
        if (failed || channel == null || !channel.isOpen())
        {
            throw new IOException("Installation-plan journal unavailable; recovery required");
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        if (channel != null)
        {
            channel.close();
            channel = null;
        }
    }
}
