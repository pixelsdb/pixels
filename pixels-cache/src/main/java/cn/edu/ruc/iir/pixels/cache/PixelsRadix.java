package cn.edu.ruc.iir.pixels.cache;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsRadix
{
    private final DynamicArray<RadixNode> nodes;    // tree nodes.

    public PixelsRadix()
    {
        this.nodes = new DynamicArray<>();
    }

    public boolean remove(PixelsCacheKey cacheKey)
    {
        return false;
    }

    public void put(PixelsCacheKey cacheKey, PixelsCacheIdx cacheItem)
    {

    }

    public void putIfAbsent(PixelsCacheKey cacheKey, PixelsCacheIdx cacheItem)
    {

    }

    public PixelsCacheIdx get(PixelsCacheKey cacheKey)
    {
        return null;
    }
}
