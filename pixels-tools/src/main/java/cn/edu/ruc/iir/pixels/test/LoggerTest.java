package cn.edu.ruc.iir.pixels.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * pixels
 *
 * @author guodong
 */
public class LoggerTest
{
    public static Logger logger = LogManager.getLogger(LoggerTest.class);

    public static void main(String[] args)
    {
        Thread[] threads = new Thread[20];
        long start = System.nanoTime();
        for (int i = 0; i < 20; i++)
        {
            threads[i] = new Thread(new LogThread(String.valueOf(i)));
            threads[i].start();
        }
        for (int i = 0; i < 20; i++)
        {
            try
            {
                threads[i].join();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        long end = System.nanoTime();
        logger.info("Total cost: " + (end - start));
    }

    static class LogThread implements Runnable
    {
        private final String id;

        public LogThread(String id)
        {
            this.id = id;
        }

        @Override
        public void run()
        {
            long sum = 0;
            long begin = System.nanoTime();
            for (int i = 0; i < 2000; i++)
            {
                sum += i;
//                logger.debug("thread " + id + ", round: " + i);
            }
            long end = System.nanoTime();
            logger.debug("[done] thread " + id + " : " + (end - begin) + ". " + sum);
        }
    }
}
