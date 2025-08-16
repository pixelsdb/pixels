/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.common.turbo;

/**
 * The virtual machine services we support.
 * Modify this enum if we add new virtual machine service support.
 *
 * @create 2023-04-10
 * @author hank
 */
public enum MachineService
{
    ec2,  // AWS EC2
    general,
    ;

    /**
     * Case-insensitive parsing from String name to enum value.
     * @param value the name of virtual machine service.
     * @return the machine service type
     */
    public static MachineService from(String value)
    {
        return valueOf(value.toLowerCase());
    }

    /**
     * Whether the value is a valid virtual machine service.
     * @return true if valid
     */
    public static boolean isValid(String value)
    {
        for (MachineService scheme : values())
        {
            if (scheme.equals(value))
            {
                return true;
            }
        }
        return false;
    }

    public boolean equals(String other)
    {
        return this.toString().equalsIgnoreCase(other);
    }

    public boolean equals(MachineService other)
    {
        // enums in Java can be compared using '=='.
        return this == other;
    }
}
