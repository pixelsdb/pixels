package io.pixelsdb.pixels.core.utils;

/**
 * dynamic int array from apache orc project
 * <p>
 * Dynamic int array that uses primitive types and chunks to avoid copying
 * large number of integers when it resizes.
 * <p>
 * The motivation for this class is memory optimization, i.e. space efficient
 * storage of potentially huge arrays without good a-priori size guesses.
 * <p>
 * The API of this class is between a primitive array and a AbstractList. It's
 * not a Collection implementation because it handles primitive types, but the
 * API could be extended to support iterators and the like.
 * <p>
 * NOTE: Like standard Collection implementations/arrays, this class is not
 * synchronized.
 */
public final class DynamicIntArray
{
    static final int DEFAULT_CHUNKSIZE = 8 * 1024;
    static final int INIT_CHUNKS = 128;

    private final int chunkSize;       // our allocation size
    private int[][] data;              // the real data
    private int length;                // max set element index +1
    private int initializedChunks = 0; // the number of created chunks

    public DynamicIntArray()
    {
        this(DEFAULT_CHUNKSIZE);
    }

    public DynamicIntArray(int chunkSize)
    {
        this.chunkSize = chunkSize;

        data = new int[INIT_CHUNKS][];
    }

    /**
     * Ensure that the given index is valid.
     */
    private void grow(int chunkIndex)
    {
        if (chunkIndex >= initializedChunks)
        {
            if (chunkIndex >= data.length)
            {
                int newSize = Math.max(chunkIndex + 1, 2 * data.length);
                int[][] newChunk = new int[newSize][];
                System.arraycopy(data, 0, newChunk, 0, data.length);
                data = newChunk;
            }
            for (int i = initializedChunks; i <= chunkIndex; ++i)
            {
                data[i] = new int[chunkSize];
            }
            initializedChunks = chunkIndex + 1;
        }
    }

    public int get(int index)
    {
        if (index >= length)
        {
            throw new IndexOutOfBoundsException("Index " + index +
                    " is outside of 0.." +
                    (length - 1));
        }
        int i = index / chunkSize;
        int j = index % chunkSize;
        return data[i][j];
    }

    public void set(int index, int value)
    {
        int i = index / chunkSize;
        int j = index % chunkSize;
        grow(i);
        if (index >= length)
        {
            length = index + 1;
        }
        data[i][j] = value;
    }

    public void increment(int index, int value)
    {
        int i = index / chunkSize;
        int j = index % chunkSize;
        grow(i);
        if (index >= length)
        {
            length = index + 1;
        }
        data[i][j] += value;
    }

    public void add(int value)
    {
        int i = length / chunkSize;
        int j = length % chunkSize;
        grow(i);
        data[i][j] = value;
        length += 1;
    }

    public int size()
    {
        return length;
    }

    public void clear()
    {
        length = 0;
        for (int i = 0; i < data.length; ++i)
        {
            data[i] = null;
        }
        initializedChunks = 0;
    }

    @Override
    public String toString()
    {
        int i;
        StringBuilder sb = new StringBuilder(length * 4);

        sb.append('{');
        int l = length - 1;
        for (i = 0; i < l; i++)
        {
            sb.append(get(i));
            sb.append(',');
        }
        sb.append(get(i));
        sb.append('}');

        return sb.toString();
    }
}
