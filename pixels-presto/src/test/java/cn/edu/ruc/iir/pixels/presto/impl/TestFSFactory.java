package cn.edu.ruc.iir.pixels.presto.impl;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.impl
 * @ClassName: TestFSFactory
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-22 21:53
 **/
public class TestFSFactory {

    @Test
    public void testListFiles() {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        FSFactory fsFactory = new FSFactory(config);
        String tablePath = "/pixels/pixels/test_105/v_0_compact";
        List<Path> files = fsFactory.listFiles(tablePath);
        for (Path s : files) {
            System.out.println(s.getName());
        }
    }

    @Test
    public void testGetBlockLocations() throws UnknownHostException {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        FSFactory fsFactory = new FSFactory(config);
        String tablePath = "pixels/db/default/test/Point.txt";
        List<HostAddress> files = fsFactory.getBlockLocations(new Path(tablePath), 0, Long.MAX_VALUE);
        System.out.println(files.size());
        for (HostAddress hs : files) {
            System.out.println(hs.toInetAddress().toString());
            System.out.println(hs.getHostText());
        }
        files.remove(0);
        System.out.println(files.size());
        HostAddress hostAddress = Iterables.getOnlyElement(files);
        System.out.println(hostAddress.toInetAddress().toString());
        System.out.println(hostAddress.getHostText());
    }

}
