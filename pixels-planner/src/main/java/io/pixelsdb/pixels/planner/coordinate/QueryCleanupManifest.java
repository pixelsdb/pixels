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
