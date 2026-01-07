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
package io.pixelsdb.pixels.common.node;

import io.pixelsdb.pixels.daemon.NodeProto;

import java.util.Objects;

/**
 * Identifier for a virtual node, used as a unique key in Maps.
 * This class is immutable to ensure consistency when used as a key.
 */
public final class VnodeIdentifier
{
    private final String address;
    private final int virtualNodeId;

    /**
     * Constructs a new identifier.
     * * @param address The network address of the node.
     * @param virtualNodeId The specific virtual ID on that host.
     */
    public VnodeIdentifier(String address, int virtualNodeId)
    {
        this.address = address;
        this.virtualNodeId = virtualNodeId;
    }

    /**
     * Factory method to create an identifier from a Protobuf NodeInfo message.
     * * @param nodeInfo The Protobuf message object.
     * @return A new instance of VnodeIdentifier.
     */
    public static VnodeIdentifier fromNodeInfo(NodeProto.NodeInfo nodeInfo)
    {
        return new VnodeIdentifier(nodeInfo.getAddress(), nodeInfo.getVirtualNodeId());
    }

    public String getAddress()
    {
        return address;
    }

    public int getVirtualNodeId()
    {
        return virtualNodeId;
    }

    /**
     * Compares this identifier with another object for equality.
     * Required for correct behavior in HashMaps.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        VnodeIdentifier that = (VnodeIdentifier) o;
        return virtualNodeId == that.virtualNodeId &&
                Objects.equals(address, that.address);
    }

    /**
     * Generates a hash code for this identifier.
     * Required for correct behavior in HashMaps.
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(address, virtualNodeId);
    }

    @Override
    public String toString()
    {
        return "VnodeIdentifier{" +
                "address='" + address + '\'' +
                ", virtualNodeId=" + virtualNodeId +
                '}';
    }
}