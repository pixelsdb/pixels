package io.pixelsdb.pixels.common.balance;

import com.facebook.presto.spi.HostAddress;
import io.pixelsdb.pixels.common.exception.BalancerException;
import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.Set;

/**
 * Created at: 19-7-28
 * Author: hank
 */
public abstract class Balancer
{
    abstract public void put (String path, HostAddress address);
    abstract public void put (String path, Set<HostAddress> addresses);
    abstract public HostAddress get (String path);
    abstract public Map<String, HostAddress> getAll();
    abstract public void balance () throws BalancerException;
    abstract public boolean isBalanced ();

    public void cascade(Balancer balancer)
    {
        if (balancer == null)
        {
            return;
        }

        Map<String, HostAddress> all = this.getAll();

        if (all != null)
        {
            for (Map.Entry<String, HostAddress> entry : all.entrySet())
            {
                balancer.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
