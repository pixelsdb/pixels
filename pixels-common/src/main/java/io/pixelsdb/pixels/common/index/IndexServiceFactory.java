package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Factory class for creating IndexService instances.
 * It chooses between RPCIndexService and LocalIndexService
 * according to the configuration.
 */
public class IndexServiceFactory
{
    private static final Logger logger = LogManager.getLogger(IndexServiceFactory.class);

    private static IndexServiceFactory instance = null;

    private final IndexService.Scheme enabledScheme;

    private IndexServiceFactory()
    {
        String value = ConfigFactory.Instance().getProperty("index.service.scheme");
        requireNonNull(value, "index.service.scheme is not configured");

        this.enabledScheme = IndexService.Scheme.from(value.trim());

        logger.info("IndexServiceFactory initialized with scheme: {}", this.enabledScheme);
    }

    public static IndexServiceFactory Instance()
    {
        if (instance == null)
        {
            instance = new IndexServiceFactory();
        }
        return instance;
    }

    /**
     * Create the IndexService instance according to the configured type.
     */
    public IndexService createInstance()
    {
        switch (this.enabledScheme)
        {
            case rpc:
                return RPCIndexService.Instance();
            case local:
                return LocalIndexService.Instance();
            default:
                throw new IllegalStateException("Unexpected scheme: " + this.enabledScheme);
        }
    }

    public IndexService.Scheme getEnabledScheme()
    {
        return this.enabledScheme;
    }
}

