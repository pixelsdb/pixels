package cn.edu.ruc.iir.pixels.core;

import java.util.concurrent.ConcurrentHashMap;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsFooterCache
{
    private final ConcurrentHashMap<String, PixelsProto.FileTail> fileTailCacheMap;
    private final ConcurrentHashMap<String, PixelsProto.RowGroupFooter> rowGroupFooterCacheMap;

    public PixelsFooterCache()
    {
        this.fileTailCacheMap = new ConcurrentHashMap<>(200);
        this.rowGroupFooterCacheMap = new ConcurrentHashMap<>(5000);
    }

    public void putFileTail(String id, PixelsProto.FileTail fileTail)
    {
        fileTailCacheMap.putIfAbsent(id, fileTail);
    }

    public PixelsProto.FileTail getFileTail(String id)
    {
        return fileTailCacheMap.get(id);
    }

    public void putRGFooter(String id, PixelsProto.RowGroupFooter footer)
    {
        rowGroupFooterCacheMap.putIfAbsent(id, footer);
    }

    public PixelsProto.RowGroupFooter getRGFooter(String id)
    {
        return rowGroupFooterCacheMap.get(id);
    }
}
