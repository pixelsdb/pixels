package cn.edu.ruc.iir.pixels.daemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;

public class Daemon implements Runnable
{
    private FileChannel myChannel = null;
    private FileChannel partnerChannel = null;
    private String[] partnerCmd = null;
    private volatile boolean running = false;
    private static Logger log = LogManager.getLogger(Daemon.class);

    public void setup (String selfFilePath, String partnerFilePath, String[] partnerCmd)
    {
        File myLockFile = new File(selfFilePath);
        File partnerLockFile = new File(partnerFilePath);
        this.partnerCmd = partnerCmd;
        if (!myLockFile.exists())
        {
            try
            {
                myLockFile.createNewFile();
            } catch (IOException e)
            {
                log.error("failed to create my own lock file.", e);
            }
        }
        if (!partnerLockFile.exists())
        {
            try
            {
                partnerLockFile.createNewFile();
            } catch (IOException e)
            {
                log.error("failed to create my partner's lock file.", e);
            }
        }
        try
        {
            this.myChannel = new FileOutputStream(myLockFile).getChannel();
            this.partnerChannel = new FileOutputStream(partnerLockFile).getChannel();

        } catch (IOException e)
        {
            log.error("I/O exception when creating lock file channels.", e);
            this.clean();
        }
    }

    public void clean ()
    {
        try
        {
            if (this.myChannel != null)
            {
                this.myChannel.close();
            }
        } catch (IOException e)
        {
            log.error("error when closing my own channel.", e);
        }
        try
        {
            if (this.partnerChannel != null)
            {
                this.partnerChannel.close();
            }
        } catch (IOException e)
        {
            log.error("error when closing my partner's channel", e);
        }
    }

    @Override
    public void run()
    {
        FileLock myLock = null;
        try
        {
            myLock = this.myChannel.tryLock();
            if (myLock == null)
            {
                // this process has been started
                this.clean();
                log.info("Such daemon process has already been started, exiting...");
                this.running = false;
                System.exit(0);
            }
            else
            {
                this.running = true;
                log.info("starting daemon...");
            }

            while (this.running)
            {
                // make sure the partner is running too.
                FileLock partnerLock = this.partnerChannel.tryLock();
                if (partnerLock != null)
                {
                    // the guarded process is not running.
                    partnerLock.release();
                    log.info("starting partner...");
                    ProcessBuilder builder = new ProcessBuilder(this.partnerCmd);
                    builder.start();
                }
                TimeUnit.SECONDS.sleep(3);
            }
        } catch (Exception e)
        {
            log.error("exception occurs when running.", e);
        }
        log.info("shutdown, exiting...");
        this.running = false;
        try
        {
            if (myLock != null)
            {
                myLock.release();
            }
        } catch (IOException e1)
        {
            log.error("error when releasing my lock.");
        }
    }

    public void shutdown()
    {
        this.running = false;
    }

    public boolean isRunning()
    {
        return this.running;
    }
}
