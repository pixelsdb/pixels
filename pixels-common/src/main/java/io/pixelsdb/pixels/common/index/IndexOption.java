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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.index.IndexProto;

public class IndexOption
{
    private int vNodeId;

    public int getVNodeId()
    {
        return vNodeId;
    }

    public void setVNodeId(int vNodeId)
    {
        this.vNodeId = vNodeId;
    }

    public IndexOption() {}

    public IndexOption(IndexProto.IndexOption option)
    {
        this.vNodeId = option.getVirtualNodeId();
    }

    public IndexProto.IndexOption toProto()
    {
        return IndexProto.IndexOption.newBuilder()
                .setVirtualNodeId(vNodeId)
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int vNodeId;

        public Builder vNodeId(int vNodeId)
        {
            this.vNodeId = vNodeId;
            return this;
        }

        public IndexOption build()
        {
            IndexOption option = new IndexOption();
            option.setVNodeId(this.vNodeId);
            return option;
        }
    }
}
