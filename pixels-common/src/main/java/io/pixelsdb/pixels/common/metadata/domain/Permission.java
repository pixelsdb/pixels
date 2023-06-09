/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.common.metadata.domain;

import io.pixelsdb.pixels.daemon.MetadataProto;

/**
 * @author hank
 * @updated 2023-06-09 moved from {@link Layout} to here.
 */
public enum Permission
{
    DISABLED,
    READ_ONLY,
    READ_WRITE,
    UNRECOGNIZED;

    public static MetadataProto.Permission convertPermission(Permission permission)
    {
        switch (permission)
        {
            case DISABLED:
                return MetadataProto.Permission.DISABLED;
            case READ_ONLY:
                return MetadataProto.Permission.READ_ONLY;
            case READ_WRITE:
                return MetadataProto.Permission.READ_WRITE;
        }
        return MetadataProto.Permission.UNRECOGNIZED;
    }

    public static MetadataProto.Permission convertPermission (short permission)
    {
        switch (permission)
        {
            case -1:
                return MetadataProto.Permission.DISABLED;
            case 0:
                return MetadataProto.Permission.READ_ONLY;
            case 1:
                return MetadataProto.Permission.READ_WRITE;
        }
        return MetadataProto.Permission.DISABLED;
    }

    public static short convertPermission (MetadataProto.Permission permission)
    {
        switch (permission)
        {
            case DISABLED:
                return -1;
            case READ_ONLY:
                return 0;
            case READ_WRITE:
                return -1;
        }
        return -1;
    }
}
