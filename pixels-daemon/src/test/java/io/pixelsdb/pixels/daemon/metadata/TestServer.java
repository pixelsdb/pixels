package io.pixelsdb.pixels.daemon.metadata;

import org.junit.Test;

/**
 * @author: tao
 * @date: Create in 2018-01-27 10:46
 **/
public class TestServer {

    @Test
    public void test() {
        MetadataServer server = new MetadataServer(18888);
        server.run();
    }
}
