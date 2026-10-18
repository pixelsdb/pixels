/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.planner.coordinate;

import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

/** Atomic, per-execution recovery evidence. Never serialize storage credentials. */
final class QueryCleanupManifest
{
    private final Path path;

    QueryCleanupManifest(Path directory, long transId)
    {
        path = directory.resolve("query-" + transId + "-" + java.util.UUID.randomUUID() + ".json");
    }

    Path getPath() { return path; }

    synchronized void write(Map<String, Object> snapshot) throws IOException
    {
        Files.createDirectories(path.getParent());
        Path temporary = Files.createTempFile(path.getParent(), ".query-", ".tmp");
        try
        {
            Files.write(temporary, JSON.toJSONString(snapshot, true).getBytes(StandardCharsets.UTF_8));
            try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(
                    temporary, java.nio.file.StandardOpenOption.WRITE))
            {
                channel.force(true);
            }
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        finally { Files.deleteIfExists(temporary); }
    }
}
