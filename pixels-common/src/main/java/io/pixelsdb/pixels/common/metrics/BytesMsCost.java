package io.pixelsdb.pixels.common.metrics;

public class BytesMsCost
{
    private long bytes;
    private double ms;

    public BytesMsCost()
    {
    }

    public BytesMsCost(long bytes, double ms)
    {
        this.bytes = bytes;
        this.ms = ms;
    }

    public void setBytes(long bytes)
    {
        this.bytes = bytes;
    }

    public void setMs(double ms)
    {
        this.ms = ms;
    }

    public long getBytes()
    {
        return bytes;
    }

    public double getMs()
    {
        return ms;
    }
}
