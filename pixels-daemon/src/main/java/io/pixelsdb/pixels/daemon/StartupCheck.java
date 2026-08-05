/*
 * Copyright 2026 PixelsDB.
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
 * You should have received a copy of the GNU Affero General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon;

/**
 * A check that must complete before a server starts serving requests.
 *
 * <p>The wait is interruptible so that a daemon shutdown can cancel a server
 * that is still waiting for one of its startup checks.</p>
 *
 * @author PixelsDB
 */
public interface StartupCheck
{
    String getDescription();

    /**
     * Wait until this startup check is satisfied or the supplied deadline expires.
     *
     * @param deadlineNanos an absolute deadline from {@link System#nanoTime()}
     * @throws InterruptedException if the wait is interrupted
     */
    void awaitReady(long deadlineNanos) throws InterruptedException;
}
