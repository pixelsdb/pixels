/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

import org.junit.Test;

public class TestShutdownHookManager
{
    @Test
    public void testShutdownHookManager()
    {
        ShutdownHookManager shutdownHookManager = ShutdownHookManager.Instance();
        shutdownHookManager.registerShutdownHook(this.getClass(), true, () -> System.out.println("Serial Shutdown Hook 1"));
        shutdownHookManager.registerShutdownHook(this.getClass(), true, () -> System.out.println("Serial Shutdown Hook 2"));
        shutdownHookManager.registerShutdownHook(this.getClass(), true, () -> System.out.println("Serial Shutdown Hook 3"));
        shutdownHookManager.registerShutdownHook(this.getClass(), true, () -> System.out.println("Serial Shutdown Hook 4"));
        shutdownHookManager.registerShutdownHook(this.getClass(), true, () -> System.out.println("Serial Shutdown Hook 5"));
        shutdownHookManager.registerShutdownHook(this.getClass(), true, () -> System.out.println("Serial Shutdown Hook 6"));
        shutdownHookManager.registerShutdownHook(this.getClass(), true, () -> System.out.println("Serial Shutdown Hook 7"));
        shutdownHookManager.registerShutdownHook(this.getClass(), true, () -> System.out.println("Serial Shutdown Hook 8"));
        shutdownHookManager.registerShutdownHook(this.getClass(), false, () -> System.out.println("Parallel Shutdown Hook 1"));
        shutdownHookManager.registerShutdownHook(this.getClass(), false, () -> System.out.println("Parallel Shutdown Hook 2"));
        shutdownHookManager.registerShutdownHook(this.getClass(), false, () -> System.out.println("Parallel Shutdown Hook 3"));
        shutdownHookManager.registerShutdownHook(this.getClass(), false, () -> System.out.println("Parallel Shutdown Hook 4"));
        shutdownHookManager.registerShutdownHook(this.getClass(), false, () -> System.out.println("Parallel Shutdown Hook 5"));
        shutdownHookManager.registerShutdownHook(this.getClass(), false, () -> System.out.println("Parallel Shutdown Hook 6"));
        shutdownHookManager.registerShutdownHook(this.getClass(), false, () -> System.out.println("Parallel Shutdown Hook 7"));
        shutdownHookManager.registerShutdownHook(this.getClass(), false, () -> System.out.println("Parallel Shutdown Hook 8"));
    }
}
