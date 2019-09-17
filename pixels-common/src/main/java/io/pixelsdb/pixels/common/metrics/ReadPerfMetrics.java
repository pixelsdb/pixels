/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.metrics;

import java.util.ArrayList;
import java.util.List;

public class ReadPerfMetrics
{
    private List<BytesMsCost> seqRead = new ArrayList<>();
    private List<BytesMsCost> seek = new ArrayList<>();
    private List<NamedCost> lambda = new ArrayList<>();

    public List<BytesMsCost> getSeqRead()
    {
        return seqRead;
    }

    public void setSeqRead(List<BytesMsCost> seqRead)
    {
        this.seqRead = seqRead;
    }

    public void addSeqRead(BytesMsCost b)
    {
        this.seqRead.add(b);
    }

    public List<BytesMsCost> getSeek()
    {
        return seek;
    }

    public void setSeek(List<BytesMsCost> seek)
    {
        this.seek = seek;
    }

    public void addSeek(BytesMsCost b)
    {
        this.seek.add(b);
    }

    public List<NamedCost> getLambda()
    {
        return lambda;
    }

    public void setLambda(List<NamedCost> lambda)
    {
        this.lambda = lambda;
    }

    public void addLambda(NamedCost c)
    {
        this.lambda.add(c);
    }

    public void clear()
    {
        seqRead.clear();
        seek.clear();
        lambda.clear();
    }
}
