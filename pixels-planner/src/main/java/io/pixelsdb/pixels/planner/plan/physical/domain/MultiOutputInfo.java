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
package io.pixelsdb.pixels.planner.plan.physical.domain;

import java.util.List;

/**
 * @author hank
 * @create 2022-06-02
 */
public class MultiOutputInfo extends OutputInfo
{
    private List<String> fileNames;

    /**
     * Default constructor for Jackson.
     */
    public MultiOutputInfo() { }

    public MultiOutputInfo(String path, StorageInfo storageInfo, boolean encoding, List<String> fileNames)
    {
        super(path, storageInfo, encoding);
        this.fileNames = fileNames;
    }

    /**
     * Get the folder that the output files are written into.
     * @return the path of the folder
     */
    @Override
    public String getPath()
    {
        return super.getPath();
    }

    /**
     * Set the folder that the output files are written into.
     */
    public void setPath(String path)
    {
        super.setPath(path);
    }

    public List<String> getFileNames()
    {
        return fileNames;
    }

    public void setFileNames(List<String> fileNames)
    {
        this.fileNames = fileNames;
    }
}
