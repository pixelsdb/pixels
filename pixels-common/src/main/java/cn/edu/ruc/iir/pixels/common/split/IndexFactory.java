package cn.edu.ruc.iir.pixels.common.split;

import java.util.HashMap;
import java.util.Map;

public class IndexFactory
{
    private static IndexFactory instance = null;

    public static IndexFactory Instance()
    {
        if (instance == null)
        {
            instance = new IndexFactory();
        }
        return instance;
    }

    private Map<IndexEntry, Index> indexCache = null;

    private IndexFactory()
    {
        this.indexCache = new HashMap<>();
    }

    public void cacheIndex(IndexEntry entry, Index index)
    {
        this.indexCache.put(entry, index);
    }

    public Index getIndex(IndexEntry entry)
    {
        return this.indexCache.get(entry);
    }

}
