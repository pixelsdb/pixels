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
