package io.pixelsdb.pixels.daemon.sink;

import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.sink.SinkProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.*;
public class SinkServer implements Server {
    private boolean running = false;
    private final SinkProvider sinkProvider;
    private final ConfigFactory config;
    public SinkServer(ConfigFactory config) {
        this.config = config;
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
        this.running = false;
        sinkProvider.shutdown();
    }

    @Override
    public void run() {
        sinkProvider.start(config);
    }
}
