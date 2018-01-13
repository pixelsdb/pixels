package cn.edu.ruc.iir.pixels.daemon;

import cn.edu.ruc.iir.pixels.common.LogFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;

public class Daemon implements Runnable
{
    private FileChannel mainChannel = null;
    private FileChannel guardChannel = null;
    private String[] guardCmd = null;

    public void setup (String mainFilePath, String guardFilePath, String[] guardCmd)
    {
        File mainLockFile = new File(mainFilePath);
        File guardLockFile = new File(guardFilePath);
        this.guardCmd = guardCmd;
        if (!mainLockFile.exists())
        {
            try
            {
                mainLockFile.createNewFile();
            } catch (IOException e)
            {
                LogFactory.Instance().getLog().error("create main lock file failed.", e);
            }
        }
        if (!guardLockFile.exists())
        {
            try
            {
                guardLockFile.createNewFile();
            } catch (IOException e)
            {
                LogFactory.Instance().getLog().error("create guard lock file failed.", e);
            }
        }
        try
        {
            mainChannel = new FileOutputStream(mainLockFile).getChannel();
            guardChannel = new FileOutputStream(guardLockFile).getChannel();

        } catch (IOException e)
        {
            LogFactory.Instance().getLog().error("I/O exception when create file channels.", e);
        }
    }

    public void clean ()
    {
        try
        {
            if (this.mainChannel != null)
            {
                this.mainChannel.close();
            }
        } catch (IOException e)
        {
            LogFactory.Instance().getLog().error("close channel A error", e);
        }
        try
        {
            if (this.guardChannel != null)
            {
                this.guardChannel.close();
            }
        } catch (IOException e)
        {
            LogFactory.Instance().getLog().error("close channel B error", e);
        }
    }

    @Override
    public void run()
    {
        try
        {
            FileLock mainLock = this.mainChannel.tryLock();
            if (mainLock == null)
            {
                // this process has been started
                this.clean();
                System.exit(0);
            }

            while (true)
            {
                FileLock guardLock = this.guardChannel.tryLock();
                if (guardLock != null)
                {
                    guardLock.release();
                    // the guarded process is not running.
                    ProcessBuilder builder = new ProcessBuilder(this.guardCmd);
                    builder.start();
                }
                TimeUnit.SECONDS.sleep(3);
            }

        } catch (Exception e)
        {
            LogFactory.Instance().getLog().error("Exception when running.", e);
        }
    }
}
