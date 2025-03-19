package io.pixelsdb.pixels.common.sink;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.Properties;

public interface SinkProvider {
    void start(ConfigFactory config);
    void shutdown();
    boolean isRunning();
}
