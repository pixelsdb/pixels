package cn.edu.ruc.iir.rainbow.common.cmd;

import java.util.Properties;

/**
 * Created by hank on 16-12-25.
 */
public interface Command
{
    public void setReceiver(Receiver receiver);

    public void execute(Properties params);
}
