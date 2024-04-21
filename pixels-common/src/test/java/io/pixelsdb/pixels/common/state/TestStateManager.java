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

import org.junit.Test;

import java.io.IOException;

/**
 * @author hank
 * @create 2024-04-20
 */
public class TestStateManager
{
    @Test
    public void testStateUpdate() throws IOException, InterruptedException
    {
        StateManager manager = new StateManager("state-1");
        manager.setState("1");

        StateWatcher watcher = new StateWatcher("state-1");
        watcher.onStateUpdate((key, value) -> {
            System.out.println("on state update:");
            System.out.println("key=" + key);
            System.out.println("value=" + value);
        });

        manager.setState("2");

        manager.setState("3");

        manager.deleteState();

        watcher.close();
        Thread.sleep(1000);
    }

    @Test
    public void testStateUpdateOrExist() throws IOException, InterruptedException
    {
        StateManager manager = new StateManager("state-1");

        StateWatcher watcher = new StateWatcher("state-1");

        watcher.onStateUpdateOrExist((key, value) -> {
            System.out.println("on state update:");
            System.out.println("key=" + key);
            System.out.println("value=" + value);
        });

        manager.setState("1");

        manager.setState("2");

        manager.setState("3");

        manager.deleteState();

        watcher.close();
        Thread.sleep(1000);
    }
}
