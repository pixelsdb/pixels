package io.pixelsdb.pixels.daemon;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Signal;
import sun.misc.SignalHandler;

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
    private ShutdownHandler shutdownHandler = null;
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
            // bind handler for SIGTERM(15) signal.
            this.shutdownHandler = new ShutdownHandler(this);
            Signal.handle(new Signal("TERM"), shutdownHandler);
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
        this.shutdownHandler.unbind();
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

            /*
            Due to that main daemon and its guard daemon are not guaranteed to receive the
            TERM signal at the same time. So that there is a case that:
            The first process receives the signal and terminates with the file lock released.
            After that but before the other process receives the signal, the other process
            obtains its partner's lock and restart its partner.
            In such a case, the main/guard daemons will not be terminated.
            To solve this problem, we make the process sleep 1 second before tries to start
            its partner. One second should be enough to send TERM signals to each daemon.
            */
            while (this.running)
            {
                // make sure the partner is running too.
                FileLock partnerLock = this.partnerChannel.tryLock();
                if (partnerLock != null)
                {
                    // the guarded process is not running.
                    TimeUnit.SECONDS.sleep(1); // make sure
                    partnerLock.release();
                    log.info("starting partner...");
                    ProcessBuilder builder = new ProcessBuilder(this.partnerCmd);
                    builder.start();
                }
                TimeUnit.SECONDS.sleep(3);
            }
        } catch (Exception e)
        {
            this.running = false;
            log.error("exception occurs when running.", e);
        }

        log.info("shutdown, exiting...");
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
        this.clean();
    }

    public void shutdown()
    {
        this.running = false;
    }

    public boolean isRunning()
    {
        return this.running;
    }

    public static class ShutdownHandler implements SignalHandler
    {
        private volatile Daemon target = null;

        public ShutdownHandler(Daemon target)
        {
            this.target = target;
        }

        public void unbind()
        {
            this.target = null;
        }

        @Override
        public void handle(Signal signal)
        {
            if (this.target != null)
            {
                this.target.shutdown();
            }
        }
    }
}
