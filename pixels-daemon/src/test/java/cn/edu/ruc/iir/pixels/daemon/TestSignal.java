package cn.edu.ruc.iir.pixels.daemon;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.concurrent.TimeUnit;

/**
 * Created at: 19-3-7
 * Author: hank
 */
public class TestSignal
{
    public static class ShutdownHandler implements SignalHandler
    {
        @Override
        public void handle(Signal signal)
        {
            System.out.println("quit...");
        }
    }

    public static void main(String[] args)
    {
        ShutdownHandler handler = new ShutdownHandler();
        Signal.handle(new Signal("TERM"), handler);

        while (true)
        {
            try
            {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}
