/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.turbo.planner.plan.physical.domain;

/**
 * @author hank
 * @date 02/06/2022
 */
public class InputInfo
{
    private String path;
    private int rgStart;
    /**
     * The number of row groups to be scanned from each file.
     */
    private int rgLength;

    /**
     * Default constructor for Jackson.
     */
    public InputInfo() { }

    /**
     * Create the input information for a Pixels file.
     *
     * @param path the path of the file
     * @param rgStart the row group id to start reading
     * @param rgLength the number of row groups to read, if it is non-positive,
     *                 it means read to the last row group in the file
     */
    public InputInfo(String path, int rgStart, int rgLength)
    {
        this.path = path;
        this.rgStart = rgStart;
        this.rgLength = rgLength;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public int getRgStart()
    {
        return rgStart;
    }

    public void setRgStart(int rgStart)
    {
        this.rgStart = rgStart;
    }

    public int getRgLength()
    {
        return rgLength;
    }

    public void setRgLength(int rgLength)
    {
        this.rgLength = rgLength;
    }
}