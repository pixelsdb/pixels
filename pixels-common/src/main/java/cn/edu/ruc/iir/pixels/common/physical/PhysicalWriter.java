package cn.edu.ruc.iir.pixels.common.physical;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public interface PhysicalWriter
        extends Closeable
{
    /**
     * Prepare the writer to ensure the length can fit into current hdfs block
     *
     * @param length length of content
     * @return starting offset after preparing. If -1, means prepare has failed,
     * due to the specified length cannot fit into current block.
     */
    long prepare(int length) throws IOException;

    /**
     * Append content to the file.
     *
     * @param buffer content buffer
     * @return start offset of content in the file.
     */
    long append(ByteBuffer buffer) throws IOException;

    /**
     * Append content to the file.
     *
     * @param buffer content buffer container
     * @param offset start offset of actual content buffer
     * @param length length of actual content buffer
     * @return start offset of content in the file.
     */
    long append(byte[] buffer, int offset, int length) throws IOException;

    /**
     * Close writer.
     */
    void close() throws IOException;

    /**
     * Flush writer.
     */
    void flush() throws IOException;
}
