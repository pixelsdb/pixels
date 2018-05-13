package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.core.PixelsProto;
import org.apache.hadoop.hdfs.tools.DFSck;

import java.util.HashMap;
import java.util.Map;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsMetaHolder
{
    private final Map<RowGroupId, PixelsProto.RowGroupFooter> metaHolder;

    public PixelsMetaHolder()
    {
        this.metaHolder = new HashMap<>();
        DFSck.main();
    }
}
