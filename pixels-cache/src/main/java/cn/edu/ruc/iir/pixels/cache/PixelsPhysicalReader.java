package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.exception.FSException;
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
    private final PixelsProto.FileTail fileTail;

    public PixelsPhysicalReader(FileSystem fs, Path path)
    {
        this.physicalReader = PhysicalReaderUtil.newPhysicalFSReader(fs, path);
        this.fileTail = readFileTail();
    }

    private PixelsProto.FileTail readFileTail()
    {
        if (physicalReader != null)
        {
            try
            {
                long fileLen = physicalReader.getFileLength();
                physicalReader.seek(fileLen - Long.BYTES);
                long fileTailOffset = physicalReader.readLong();
                int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
                physicalReader.seek(fileTailOffset);
                byte[] fileTailBuffer = new byte[fileTailLength];
                physicalReader.readFully(fileTailBuffer);
                return PixelsProto.FileTail.parseFrom(fileTailBuffer);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return null;
    }

    public PixelsProto.RowGroupFooter readRowGroupFooter(int rowGroupId)
            throws IOException
    {
        PixelsProto.RowGroupInformation rgInfo = fileTail.getFooter().getRowGroupInfos(rowGroupId);
        long rgFooterOffset = rgInfo.getFooterOffset();
        int rgFooterLength = rgInfo.getFooterLength();
        byte[] rgFooterBytes = new byte[rgFooterLength];
        physicalReader.seek(rgFooterOffset);
        physicalReader.readFully(rgFooterBytes);

        return PixelsProto.RowGroupFooter.parseFrom(rgFooterBytes);
    }

    public byte[] read(long offset, int length)
            throws IOException
    {
        byte[] content = new byte[length];
        physicalReader.seek(offset);
        physicalReader.readFully(content);

        return content;
    }

    public long getCurrentBlockId()
            throws FSException
    {
        return physicalReader.getCurrentBlockId();
    }
}
