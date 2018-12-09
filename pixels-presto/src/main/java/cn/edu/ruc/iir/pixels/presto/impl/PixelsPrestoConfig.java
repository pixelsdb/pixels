package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import com.facebook.presto.spi.PrestoException;
import io.airlift.configuration.Config;
import io.airlift.log.Logger;

import javax.validation.constraints.NotNull;
import java.io.IOException;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_CONFIG_ERROR;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.impl
 * @ClassName: PixelsPrestoConfig
 * @Description: Configuration read from etc/catalog/pixels-presto.properties
 * @author: tao
 * @date: Create in 2018-01-20 11:16
 **/
public class PixelsPrestoConfig
{
    private Logger logger = Logger.get(PixelsPrestoConfig.class);
    private ConfigFactory configFactory = null;
    private FSFactory fsFactory = null;

    private String pixelsHome = null;

    @Config("pixels.home")
    public PixelsPrestoConfig setPixelsHome (String pixelsHome)
    {
        this.pixelsHome = pixelsHome;

        // reload configuration
        if (this.configFactory == null)
        {
            if (pixelsHome == null || pixelsHome.isEmpty())
            {
                String defaultPixelsHome = ConfigFactory.Instance().getProperty("pixels.home");
                if (defaultPixelsHome == null)
                {
                    logger.info("use pixels.properties insided in jar.");
                } else
                {
                    logger.info("use pixels.properties under default pixels.home: " + defaultPixelsHome);
                }
            } else
            {
                if (!(pixelsHome.endsWith("/") || pixelsHome.endsWith("\\")))
                {
                    pixelsHome += "/";
                }
                try
                {
                    ConfigFactory.Instance().loadProperties(pixelsHome + "pixels.properties");
                    ConfigFactory.Instance().addProperty("pixels.home", pixelsHome);
                    logger.info("use pixels.properties under connector specified pixels.home: " + pixelsHome);

                } catch (IOException e)
                {
                    logger.error(e,"can not load pixels.properties under: " + pixelsHome +
                            ", configuration reloading is skipped.");
                    throw new PrestoException(PIXELS_CONFIG_ERROR, e);
                }
            }

            this.configFactory = ConfigFactory.Instance();
            try
            {
                this.fsFactory = FSFactory.Instance(this.configFactory.getProperty("hdfs.config.dir"));
            } catch (FSException e)
            {
                throw new PrestoException(PIXELS_CONFIG_ERROR, e);
            }

        }
        return this;
    }

    @NotNull
    public String getPixelsHome ()
    {
        return this.pixelsHome;
    }

    /**
     * Injected class should get ConfigFactory instance by this method instead of ConfigFactory.Instance().
     * @return
     */
    @NotNull
    public ConfigFactory getConfigFactory()
    {
        return this.configFactory;
    }

    /**
     * Injected class should get FSFactory instance by this method instead of FSFactory.Instance(...).
     * @return
     */
    @NotNull
    public FSFactory getFsFactory()
    {
        return this.fsFactory;
    }
}
