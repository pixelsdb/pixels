package io.pixelsdb.pixels.common;

import com.facebook.presto.spi.HostAddress;
import io.pixelsdb.pixels.common.balance.AbsoluteBalancer;
import io.pixelsdb.pixels.common.balance.Balancer;
import io.pixelsdb.pixels.common.balance.ReplicaBalancer;
import io.pixelsdb.pixels.common.exception.BalancerException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created at: 18-11-26
 * Author: hank
 */
public class TestBalancer
{
    @Test
    public void testAbsolute () throws BalancerException
    {
        Balancer balancer = new AbsoluteBalancer();

        balancer.put(("path0"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path1"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path2"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path3"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path4"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path5"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path6"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path7"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path8"), HostAddress.fromParts("node1", 9000));

        balancer.put(("path9"), HostAddress.fromParts("node2", 9000));
        balancer.put(("path10"), HostAddress.fromParts("node2", 9000));
        balancer.put(("path11"), HostAddress.fromParts("node2", 9000));

        balancer.put(("path12"), HostAddress.fromParts("node3", 9000));
        balancer.put(("path13"), HostAddress.fromParts("node3", 9000));

        balancer.put(("path14"), HostAddress.fromParts("node4", 9000));
        balancer.put(("path15"), HostAddress.fromParts("node4", 9000));
        balancer.put(("path16"), HostAddress.fromParts("node4", 9000));
        balancer.put(("path17"), HostAddress.fromParts("node4", 9000));
        balancer.put(("path18"), HostAddress.fromParts("node4", 9000));

        //balancer.put(("path19"), HostAddress.fromParts("node5", 9000));

        balancer.balance();

        System.out.println(balancer.isBalanced());

        for (int i = 0; i < 19; ++i)
        {
            String path = "path" + i;
            System.out.println(balancer.get(path).toString());
        }
    }

    @Test
    public void testReplica () throws BalancerException
    {
        List<HostAddress> nodes = new ArrayList<>();
        nodes.add(HostAddress.fromParts("node1", 9000));
        nodes.add(HostAddress.fromParts("node2", 9000));
        nodes.add(HostAddress.fromParts("node3", 9000));
        nodes.add(HostAddress.fromParts("node4", 9000));
        Balancer balancer = new ReplicaBalancer(nodes);

        balancer.put(("path0"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path0"), HostAddress.fromParts("node2", 9000));
        balancer.put(("path0"), HostAddress.fromParts("node3", 9000));
        balancer.put(("path1"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path1"), HostAddress.fromParts("node2", 9000));
        balancer.put(("path1"), HostAddress.fromParts("node3", 9000));
        balancer.put(("path2"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path2"), HostAddress.fromParts("node2", 9000));
        balancer.put(("path2"), HostAddress.fromParts("node4", 9000));
        balancer.put(("path3"), HostAddress.fromParts("node2", 9000));
        balancer.put(("path3"), HostAddress.fromParts("node3", 9000));
        balancer.put(("path3"), HostAddress.fromParts("node4", 9000));
        balancer.put(("path4"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path4"), HostAddress.fromParts("node3", 9000));
        balancer.put(("path4"), HostAddress.fromParts("node4", 9000));
        balancer.put(("path5"), HostAddress.fromParts("node1", 9000));
        balancer.put(("path5"), HostAddress.fromParts("node2", 9000));
        balancer.put(("path5"), HostAddress.fromParts("node3", 9000));

        balancer.balance();

        System.out.println(balancer.isBalanced());

        for (int i = 0; i < 5; ++i)
        {
            String path = "path" + i;
            System.out.println(balancer.get(path).toString());
        }
    }
}
