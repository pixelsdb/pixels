/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.state;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The watcher of a state stored in Etcd.
 * @author hank
 * @create 2024-04-21
 */
public class StateWatcher implements Closeable
{
    private static final Logger logger = LogManager.getLogger(StateWatcher.class);

    private final String key;
    private final List<Watch.Watcher> watchers;

    /**
     * Create a watcher for the state of a key.
     * @param key the key
     */
    public StateWatcher(String key)
    {
        this.key = requireNonNull(key, "key is null");
        this.watchers = new LinkedList<>();
    }

    /**
     * Set the action for the update event of the state.
     * @param action the action
     */
    public void onStateUpdate(Action action)
    {
        Watch.Watcher watcher = EtcdUtil.Instance().getWatchClient().watch(
                ByteSequence.from(key, StandardCharsets.UTF_8),
                WatchOption.DEFAULT, watchResponse -> {
                    for (WatchEvent event : watchResponse.getEvents())
                    {
                        System.out.println("1" + event.getEventType().toString());
                        if (event.getEventType() == WatchEvent.EventType.PUT)
                        {
                            try
                            {
                                KeyValue current = requireNonNull(event.getKeyValue(),
                                        "the current key value should not be null");
                                action.perform(
                                        current.getKey().toString(StandardCharsets.UTF_8),
                                        current.getValue().toString(StandardCharsets.UTF_8));
                            }
                            catch (Exception e)
                            {
                                logger.error("no exception should be caught here", e);
                                e.printStackTrace();
                            }
                        }
                    }
                });
        this.watchers.add(watcher);
    }

    /**
     * Close this state watcher.
     * @throws IOException if some state watchers are closed exceptionally.
     */
    @Override
    public void close() throws IOException
    {
        for (Watch.Watcher watcher : this.watchers)
        {
            watcher.close();
        }
    }
}
