/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manager to maintain MainIndex instances for each tableId.
 *
 * @author Rolland1944
 * @create 2025-06-24
 */
public class MainIndexManager implements Closeable
{
    // Mapping from tableId to MainIndex
    private final Map<Long, MainIndex> indexMap = new ConcurrentHashMap<>();

    private final MainIndexFactory mainIndexFactory;

    /**
     * MainIndexManager requires a factory to create MainIndex for a given tableId.
     * @param mainIndexFactory the factory instance
     */
    public MainIndexManager(MainIndexFactory mainIndexFactory)
    {
        this.mainIndexFactory = mainIndexFactory;
    }

    /**
     * Get or create the MainIndex associated with a table.
     * @param tableId the id of the table
     * @return the MainIndex instance
     */
    public MainIndex getOrCreate(long tableId)
    {
        return indexMap.computeIfAbsent(tableId, mainIndexFactory::create);
    }

    /**
     * Manually register an existing MainIndex (optional usage).
     * @param tableId the table id
     * @param mainIndex the MainIndex to register
     * @throws IllegalStateException if already registered
     */
    public void register(long tableId, MainIndex mainIndex)
    {
        if (indexMap.putIfAbsent(tableId, mainIndex) != null) {
            throw new IllegalStateException("MainIndex already registered for tableId: " + tableId);
        }
    }

    /**
     * Remove and close the MainIndex for a specific table.
     * @param tableId the table id
     * @throws IOException if close fails
     */
    public void remove(long tableId) throws IOException
    {
        MainIndex removed = indexMap.remove(tableId);
        if (removed != null) {
            removed.close();
        }
    }

    /**
     * Close all managed MainIndex instances.
     * @throws IOException if any close operation fails
     */
    @Override
    public void close() throws IOException
    {
        IOException exception = null;
        for (MainIndex index : indexMap.values()) {
            try {
                index.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }
        indexMap.clear();

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Factory interface to support pluggable MainIndex creation.
     */
    @FunctionalInterface
    public interface MainIndexFactory
    {
        MainIndex create(long tableId);
    }
}
