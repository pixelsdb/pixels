package cn.edu.ruc.iir.pixels.presto;

import com.facebook.presto.spi.HostAddress;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Created at: 18-11-26
 * Author: hank
 */
public class TestBalancer
{
    @Test
    public void test ()
    {
        PixelsSplitManager.Balancer balancer = new PixelsSplitManager.Balancer();

        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path0"));
        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path1"));
        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path2"));
        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path3"));
        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path4"));
        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path5"));
        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path6"));
        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path7"));
        balancer.put(HostAddress.fromParts("node1", 9000), new Path("path8"));

        balancer.put(HostAddress.fromParts("node2", 9000), new Path("path9"));
        balancer.put(HostAddress.fromParts("node2", 9000), new Path("path10"));
        balancer.put(HostAddress.fromParts("node2", 9000), new Path("path11"));

        balancer.put(HostAddress.fromParts("node3", 9000), new Path("path12"));
        balancer.put(HostAddress.fromParts("node3", 9000), new Path("path13"));

        balancer.put(HostAddress.fromParts("node4", 9000), new Path("path14"));
        balancer.put(HostAddress.fromParts("node4", 9000), new Path("path15"));
        balancer.put(HostAddress.fromParts("node4", 9000), new Path("path16"));
        balancer.put(HostAddress.fromParts("node4", 9000), new Path("path17"));
        balancer.put(HostAddress.fromParts("node4", 9000), new Path("path18"));

        //balancer.put(HostAddress.fromParts("node5", 9000), new Path("path19"));

        balancer.balance();

        System.out.println(balancer.isBalanced());

        for (int i = 0; i < 19; ++i)
        {
            Path path = new Path("path" + i);
            System.out.println(balancer.get(path).toString());
        }
    }
}
