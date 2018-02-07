package cn.edu.ruc.iir.pixels.daemon.metric;

public class BytesMsCost
{
    private long bytes;
    private double ms;

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
