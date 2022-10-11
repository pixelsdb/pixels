package io.pixelsdb.pixels.cache;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface CacheReader {
    PixelsCacheIdx search(PixelsCacheKey key);
    default PixelsCacheIdx search(long blockId, short rowGroupId, short columnId) {
        return search(new PixelsCacheKey(blockId, rowGroupId, columnId));
    }
    // this has a problem that the buf might not be enough to hold the result
    // we can give caller a return value to indicate this situation, but it will involve several calls
    // on the get, which might not be good; but we can do a optimization to make the CacheReader stateful
    // to memorize the last called key.
    // or the input parameter could be a DynamicArray which resizes itself.
    // TODO: caller should make sure the size is sufficient, we do the experiment first with this api
    int get(PixelsCacheKey key, byte[] buf, int size) throws IOException;

    // Note: this is not safe, as the writer might begin to write the returned ByteBuffer
    ByteBuffer getZeroCopy(PixelsCacheKey key) throws IOException;
}
