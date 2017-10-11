package cn.edu.ruc.iir.rainbow.common.cmd;

import java.util.Properties;

/**
 * Created by hank on 17-5-4.
 */
public interface Receiver
{
    /**
     * percentage is in range of (0, 1).
     * e.g. percentage=0.123 means 12.3%.
     * @param percentage
     */
    public void progress(double percentage);

    public void action(Properties results);
}
