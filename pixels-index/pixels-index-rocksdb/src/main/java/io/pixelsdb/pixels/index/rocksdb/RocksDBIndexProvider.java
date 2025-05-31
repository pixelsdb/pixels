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
package io.pixelsdb.pixels.index.rocksdb;

import io.pixelsdb.pixels.common.index.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.annotation.Nonnull;
import java.io.IOException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.rocksdb.RocksDBException;

/**
 * @author hank, Rolland1944
 * @create 2025-02-09
 */
public class RocksDBIndexProvider implements SecondaryIndexProvider
{
    private static final Logger logger = LogManager.getLogger(RocksDBIndexProvider.class);
    private final MainIndex mainIndex = new MainIndexImpl();
    private final String RocksdbPath = ConfigFactory.Instance().getProperty("RocksdbPath");
    @Override
    public SecondaryIndex createInstance(@Nonnull SecondaryIndex.Scheme scheme) throws IOException {
        if (scheme == SecondaryIndex.Scheme.rocksdb) {
            try {
                return new RocksDBIndex(RocksdbPath,mainIndex);
            } catch (RocksDBException e) {
                logger.error("Failed to create RocksDB instance", e);
                return null;
            }
        }
        throw new IllegalArgumentException("Unsupported scheme: " + scheme);
    }

    @Override
    public boolean compatibleWith(@Nonnull SecondaryIndex.Scheme scheme) {
        return scheme == SecondaryIndex.Scheme.rocksdb;
    }
}
