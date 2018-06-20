package cn.edu.ruc.iir.pixels.presto.split.index;

import java.util.HashMap;
import java.util.Map;

public class IndexFactory {
    private static IndexFactory instance = null;

    public static IndexFactory Instance() {
        if (instance == null) {
            instance = new IndexFactory();
        }
        return instance;
    }

    private Map<IndexEntry, Index> indexCache = null;

    private IndexFactory() {
        this.indexCache = new HashMap<>();
    }

    public void cacheIndex(IndexEntry name, Index inverted) {
        this.indexCache.put(name, inverted);
    }

    public Index getIndex(IndexEntry name) {
        return this.indexCache.get(name);
    }

}
