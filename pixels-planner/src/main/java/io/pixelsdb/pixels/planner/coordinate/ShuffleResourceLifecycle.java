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

import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;

import java.io.IOException;
import java.util.Collection;

/**
 * Owns external resources used by the shuffle edges of one query execution.
 *
 * Implementations are created per query. This keeps AWS resource operations
 * outside the general stage/task coordinator while preserving one lifecycle
 * owner for prepare and cleanup.
 */
public interface ShuffleResourceLifecycle
{
    void prepare(Collection<ShuffleInfo> shuffleInfos) throws IOException;

    void cleanup() throws IOException;
}
