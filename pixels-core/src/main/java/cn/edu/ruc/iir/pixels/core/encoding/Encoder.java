package cn.edu.ruc.iir.pixels.core.encoding;

import cn.edu.ruc.iir.pixels.core.exception.PixelsEncodingException;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class Encoder
{
    public byte[] encode(boolean[] values)
    {
        throw new PixelsEncodingException("Encoding boolean values is not supported");
    }

    public byte[] encode(boolean[] values, long offset, long length)
    {
        throw new PixelsEncodingException("Encoding boolean values is not supported");
    }

    public byte[] encode(char[] values)
    {
        throw new PixelsEncodingException("Encoding char values is not supported");
    }

    public byte[] encode(char[] values, long offset, long length)
    {
        throw new PixelsEncodingException("Encoding char values is not supported");
    }

    public byte[] encode(byte[] values) throws IOException
    {
        throw new PixelsEncodingException("Encoding byte values is not supported");
    }

    public byte[] encode(byte[] values, long offset, long length) throws IOException
    {
        throw new PixelsEncodingException("Encoding byte values is not supported");
    }

    public byte[] encode(long[] values) throws IOException
    {
        throw new PixelsEncodingException("Encoding long values is not supported");
    }

    public byte[] encode(long[] values, long offset, long length) throws IOException
    {
        throw new PixelsEncodingException("Encoding long values is not supported");
    }

    public byte[] encode(int[] values)
    {
        throw new PixelsEncodingException("Encoding int values is not supported");
    }

    public byte[] encode(short[] values)
    {
        throw new PixelsEncodingException("Encoding short values is not supported");
    }

    public byte[] encode(float[] values)
    {
        throw new PixelsEncodingException("Encoding float values is not supported");
    }

    public byte[] encode(double[] values)
    {
        throw new PixelsEncodingException("Encoding double values is not supported");
    }

    public void close() throws IOException
    {}
}
