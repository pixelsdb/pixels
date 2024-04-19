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

import java.util.concurrent.CompletableFuture;

/**
 * @author hank
 * @create 2024-04-19
 */
public class StateManager
{
    private String key;

    /**
     * Create a state manager for the state with a key.
     * @param key the key
     */
    public StateManager(String key)
    {
        this.key = key;
    }

    /**
     * Update the state by setting a value for the state key.
     * @param value the value
     */
    public void setState(String value)
    {

    }

    /**
     * Delete the state key-value pair.
     */
    public void deleteState()
    {

    }

    /**
     * Set the action for the update event of the state.
     * @param action the action
     * @return the result of performing the action.
     */
    public CompletableFuture<ActionResult> onStateUpdate(Action action)
    {
        return null;
    }

    /**
     * Set the action for the delete event of the state.
     * @param action the action
     * @return the result of performing the action
     */
    public CompletableFuture<ActionResult> onStateDelete(Action action)
    {
        return null;
    }
}
