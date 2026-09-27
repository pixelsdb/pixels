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
package io.pixelsdb.pixels.common.ingest.durable;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;

/** Checksummed, bounded, exclusively owned LOCAL state with atomic replacement. */
public final class AtomicStateFile implements Closeable {
    private final Path directory, file;
    private final int limit;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private boolean closed, failed;

    public AtomicStateFile(Path directory, int limit) throws IOException {
        if (limit <= 0) throw new IllegalArgumentException("limit");
        this.directory = directory;
        this.file = directory.resolve("state");
        this.limit = limit;
        Files.createDirectories(directory);
        lockChannel =
                FileChannel.open(
                        directory.resolve("owner.lock"),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE);
        try {
            lock = lockChannel.tryLock();
            if (lock == null) throw new IOException("State volume already owned");
        } catch (Exception e) {
            lockChannel.close();
            throw new IOException("Cannot own state volume", e);
        }
    }

    private void open() throws IOException {
        if (closed || failed) throw new IOException("State unavailable; recovery required");
    }

    private static byte[] digest(byte[] b) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(b);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    public synchronized byte[] read() throws IOException {
        open();
        if (!Files.exists(file)) return new byte[0];
        long size = Files.size(file);
        if (size < 40 || size > 40L + limit) throw new IOException("Invalid state size");
        byte[] all = Files.readAllBytes(file);
        ByteBuffer b = ByteBuffer.wrap(all);
        if (b.getInt() != 0x50585331) throw new IOException("Invalid state magic");
        int n = b.getInt();
        if (n < 0 || n != all.length - 40) throw new IOException("Invalid state length");
        byte[] p = new byte[n], h = new byte[32];
        b.get(p);
        b.get(h);
        if (!MessageDigest.isEqual(h, digest(p))) throw new IOException("State checksum mismatch");
        return p;
    }

    public synchronized void store(byte[] payload) throws IOException {
        open();
        if (payload.length > limit) throw new IOException("State capacity exceeded");
        Path temp = directory.resolve("state.new");
        try {
            ByteBuffer b =
                    ByteBuffer.allocate(payload.length + 40)
                            .putInt(0x50585331)
                            .putInt(payload.length)
                            .put(payload)
                            .put(digest(payload));
            b.flip();
            try (FileChannel channel =
                    FileChannel.open(
                            temp,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING,
                            StandardOpenOption.WRITE)) {
                while (b.hasRemaining()) channel.write(b);
                channel.force(true);
            }
            Files.move(
                    temp,
                    file,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
                channel.force(true);
            }
        } catch (IOException e) {
            failed = true;
            throw e;
        }
    }

    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                lock.release();
            } finally {
                lockChannel.close();
            }
        }
    }
}
