package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.physical.PhysicalReader;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalReaderUtil;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsPhysicalReader
{
    private final PhysicalReader physicalReader;

    public PixelsPhysicalReader(FileSystem fs, Path path)
    {
        this.physicalReader = PhysicalReaderUtil.newPhysicalFSReader(fs, path);
    }

    public PixelsProto.FileTail readFileTail()
    {
        return null;
    }

    public PixelsProto.RowGroupFooter readRowGroupFooter(int rowGroupId)
    {
        return null;
    }

    public byte[] read(long offset, int length)
            throws IOException
    {
        byte[] content = new byte[length];
        physicalReader.seek(offset);
        physicalReader.readFully(content);

        return content;
    }
}
