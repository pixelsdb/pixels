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
package io.pixelsdb.pixels.planner;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Path;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A query-scoped, lazy view of the regular and virtual files under data paths.
 * Each path is read from metadata at most once.
 */
public class TableFileSnapshot
{
    public static class FileInPath
    {
        private final Path dirPath;
        private final File file;

        private FileInPath(Path dirPath, File file)
        {
            this.dirPath = dirPath;
            this.file = file;
        }

        public File getFile()
        {
            return this.file;
        }

        public String getFilePath()
        {
            return File.getFilePath(this.dirPath, this.file);
        }
    }

    private final MetadataService metadataService;
    private final Map<Long, List<FileInPath>> filesByPathId = new LinkedHashMap<>();

    public TableFileSnapshot(MetadataService metadataService)
    {
        this.metadataService = requireNonNull(metadataService, "metadataService is null");
    }

    public List<String> regularFilePaths(List<Path> dirPaths) throws MetadataException
    {
        ImmutableList.Builder<String> filePaths = ImmutableList.builder();
        for (FileInPath fileInPath : files(dirPaths))
        {
            if (fileInPath.file.getType() == File.Type.REGULAR)
            {
                filePaths.add(fileInPath.getFilePath());
            }
        }
        return filePaths.build();
    }

    /**
     * Load the given paths on first access and return their regular and virtual files.
     */
    public List<FileInPath> files(List<Path> dirPaths) throws MetadataException
    {
        ImmutableList.Builder<FileInPath> files = ImmutableList.builder();
        for (Path dirPath : dirPaths)
        {
            files.addAll(filesOf(dirPath));
        }
        return files.build();
    }

    private List<FileInPath> filesOf(Path dirPath) throws MetadataException
    {
        List<FileInPath> filesInPath = this.filesByPathId.get(dirPath.getId());
        if (filesInPath == null)
        {
            List<File> files = this.metadataService.getFilesByType(
                    dirPath.getId(), EnumSet.of(File.Type.REGULAR, File.Type.VIRTUAL));
            ImmutableList.Builder<FileInPath> builder =
                    ImmutableList.builderWithExpectedSize(files.size());
            for (File file : files)
            {
                builder.add(new FileInPath(dirPath, file));
            }
            filesInPath = builder.build();
            this.filesByPathId.put(dirPath.getId(), filesInPath);
        }
        return filesInPath;
    }
}
