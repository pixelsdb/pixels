package io.pixelsdb.pixels.daemon.monitor;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class MetricsQueue {
    public static BlockingDeque<Integer> queue = new LinkedBlockingDeque<>();
}
