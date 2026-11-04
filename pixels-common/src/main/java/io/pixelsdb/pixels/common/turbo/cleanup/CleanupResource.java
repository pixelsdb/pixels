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

import static java.util.Objects.requireNonNull;

/** An exact, query-owned resource. Credentials and shared infrastructure do not belong here. */
public final class CleanupResource
{
    private final String type;
    private final String location;

    public CleanupResource(String type, String location)
    {
        this.type = requireNonNull(type, "type is null");
        this.location = requireNonNull(location, "location is null");
        if (type.isEmpty() || location.isEmpty()) { throw new IllegalArgumentException("empty resource"); }
    }

    public String getType() { return type; }
    public String getLocation() { return location; }
}
