package cn.edu.ruc.iir.pixels.presto.impl;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

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
    private String hdfsConfigDir = null;
    private String metadataServerUri = null;

    @Config("hdfs.config.dir")
    public PixelsPrestoConfig setHdfsConfigDir (String hdfsConfigDir)
    {
        this.hdfsConfigDir = hdfsConfigDir;
        return this;
    }

    @Config("metadata.server.uri")
    public PixelsPrestoConfig setMetadataServerUri (String metadataServerUri)
    {
        this.metadataServerUri = metadataServerUri;
        return this;
    }

    @NotNull
    public String getHdfsConfigDir ()
    {
        return this.hdfsConfigDir;
    }

    @NotNull
    public String getMetadataServerUri ()
    {
        return this.metadataServerUri;
    }

}
