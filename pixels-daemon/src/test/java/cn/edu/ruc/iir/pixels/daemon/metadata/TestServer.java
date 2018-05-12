package cn.edu.ruc.iir.pixels.daemon.metadata;

import org.junit.Test;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.util.server
 * @ClassName: TestServer
 * @Description:
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
