package cn.edu.ruc.iir.pixels.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class DynamicArray<T>
{
    private static final int DEFAULT_CAPACITY = 1024;
    private int capcity;

    public DynamicArray()
    {
        this(DEFAULT_CAPACITY);
    }

    public DynamicArray(int capcity)
    {
        this.capcity = capcity;
    }

    /**
     * Add element.
     * */
    public void add(T v)
    {}

    /**
     * Set value of the specified index.
     * */
    public void set(int index, T v)
    {}

    /**
     * Get value of the specified index.
     * */
    public T get(int index)
    {
        return null;
    }
}
