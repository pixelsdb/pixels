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

import javax.annotation.Nullable;

/**
 * @author hank
 * @create 2024-04-19
 */
public interface Action
{
    /**
     * Perform the action for the target event, with the key and the current and previous values of the state.
     * This method should not throw any exception.
     * @param key the key of the state
     * @param curValue the current value of the state
     * @param preValue the previous value of the state
     */
    void perform(@Nullable String key, @Nullable String curValue, @Nullable String preValue);
}
