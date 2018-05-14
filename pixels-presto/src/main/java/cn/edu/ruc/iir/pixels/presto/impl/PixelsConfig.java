package cn.edu.ruc.iir.pixels.presto.impl;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.impl
 * @ClassName: PixelsConfig
 * @Description: Configuration read from etc/catalog/pixels.properties
 * @author: tao
 * @date: Create in 2018-01-20 11:16
 **/
public class PixelsConfig {

    //read from config
    private String dbPath;
    private String tablePath;
    private String storePath;

    @NotNull
    public String getDbPath() {
        return dbPath;
    }

    @Config("pixels-store")
    public PixelsConfig setDbPath(String dbPath) {
        this.dbPath = dbPath;
        return this;
    }

    @NotNull
    public String getTablePath() {
        return tablePath;
    }

    @Config("pixels-store")
    public PixelsConfig setTablePath(String tablePath) {
        this.tablePath = tablePath;
        return this;
    }

    @NotNull
    public String getStorePath() {
        return storePath;
    }

    @Config("pixels-store")
    public PixelsConfig setStorePath(String storePath) {
        this.storePath = storePath;
        return this;
    }

    public String getHDFSWarehouse() {
        ConfigFactory config = ConfigFactory.Instance();
        String warehouse = config.getProperty("pixels.warehouse.path");
        return warehouse;
    }
}
