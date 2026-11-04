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
package io.pixelsdb.pixels.common.turbo.cleanup;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Atomic documents on a durable filesystem or an object store with atomic object publication. */
public final class CleanupJournal
{
    private final Path directory;
    private final Storage storage;
    private final String prefix;

    public CleanupJournal(Path directory)
    {
        this.directory = directory;
        this.storage = null;
        this.prefix = null;
    }

    public CleanupJournal(Storage storage, String prefix)
    {
        this.directory = null;
        this.storage = storage;
        this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
    }

    public static CleanupJournal open(String location) throws IOException
    {
        if (location.startsWith("s3://"))
        {
            return new CleanupJournal(StorageFactory.Instance().getStorage(Storage.Scheme.s3), location);
        }
        if (location.contains("://"))
        {
            throw new IOException("cleanup journal requires a filesystem path or s3:// prefix");
        }
        return new CleanupJournal(Paths.get(location));
    }

    public JSONObject read(String name) throws IOException
    {
        if (!exists(name)) { return null; }
        byte[] bytes;
        if (directory != null) { bytes = Files.readAllBytes(directory.resolve(name)); }
        else
        {
            try (InputStream input = storage.open(prefix + name);
                 ByteArrayOutputStream output = new ByteArrayOutputStream())
            {
                byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) != -1) { output.write(buffer, 0, count); }
                bytes = output.toByteArray();
            }
        }
        try { return JSON.parseObject(new String(bytes, StandardCharsets.UTF_8)); }
        catch (RuntimeException failure) { throw new IOException("invalid cleanup record: " + name, failure); }
    }

    public boolean exists(String name) throws IOException
    {
        return directory != null ? Files.exists(directory.resolve(name)) : storage.exists(prefix + name);
    }

    public String getLocation() { return directory != null ? directory.toAbsolutePath().toString() : prefix; }

    boolean coveredBy(String folder)
    {
        if (directory == null) { return prefix.startsWith(folder); }
        if (!folder.startsWith("file://")) { return false; }
        return directory.toAbsolutePath().normalize().startsWith(Paths.get(java.net.URI.create(folder)).normalize());
    }

    public void write(String name, JSONObject value) throws IOException
    {
        byte[] bytes = value.toJSONString().getBytes(StandardCharsets.UTF_8);
        if (directory == null)
        {
            // Publish a record with one object PUT. Tiny multipart parts are invalid on S3.
            try (OutputStream output = storage.create(prefix + name, true, Math.max(8192, bytes.length + 1)))
            { output.write(bytes); }
            return;
        }
        Files.createDirectories(directory);
        Path temporary = Files.createTempFile(directory, ".cleanup-", ".tmp");
        try
        {
            Files.write(temporary, bytes);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) { channel.force(true); }
            Files.move(temporary, directory.resolve(name), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) { channel.force(true); }
        }
        finally { Files.deleteIfExists(temporary); }
    }

    public List<String> tasks() throws IOException
    {
        if (directory == null)
        {
            return storage.listPaths(prefix).stream().map(path -> path.substring(path.lastIndexOf('/') + 1))
                    .filter(name -> name.endsWith(".resources.json")).collect(Collectors.toList());
        }
        Files.createDirectories(directory);
        try (Stream<Path> paths = Files.list(directory))
        {
            return paths.map(path -> path.getFileName().toString()).filter(name -> name.endsWith(".resources.json"))
                    .collect(Collectors.toList());
        }
    }
}
