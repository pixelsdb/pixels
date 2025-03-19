package io.pixelsdb.pixels.daemon.sink;

import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.sink.SinkProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.kerby.config.Conf;

import java.util.*;
public class SinkServer implements Server {
    private final SinkProvider sinkProvider;
    public SinkServer() {
        ServiceLoader<SinkProvider> sinkProviders = ServiceLoader.load(SinkProvider.class);
        for(SinkProvider sinkProvider: sinkProviders) {
            this.sinkProvider = sinkProvider;
            return;
        }
        sinkProvider = null;
    }


    @Override
    public boolean isRunning() {
        return sinkProvider.isRunning();
    }

    @Override
    public void shutdown() {
        sinkProvider.shutdown();
    }

    @Override
    public void run() {
        sinkProvider.start(ConfigFactory.Instance());
    }
}
