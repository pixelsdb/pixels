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
package io.pixelsdb.pixels.executor.lambda.domain;

/**
 * @author hank
 * @date 02/06/2022
 */
public class OutputInfo
{
    /**
     * The path that the output file is written into.
     */
    private String path;
    /**
     * Whether the serverless worker should generate a random file name.
     * If true, {@link #path} is used as the folder of the output file.
     */
    private boolean randomFileName;
    /**
     * The information of the storage endpoint.
     */
    private StorageInfo storageInfo;
    /**
     * Whether the output file should be encoded.
     */
    private boolean encoding;

    /**
     * Default constructor for Jackson.
     */
    public OutputInfo() { }

    public OutputInfo(String path, boolean randomFileName, StorageInfo storageInfo, boolean encoding)
    {
        this.path = path;
        this.randomFileName = randomFileName;
        this.storageInfo = storageInfo;
        this.encoding = encoding;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    public boolean isRandomFileName()
    {
        return randomFileName;
    }

    public void setRandomFileName(boolean randomFileName)
    {
        this.randomFileName = randomFileName;
    }

    public StorageInfo getStorageInfo()
    {
        return storageInfo;
    }

    public void setStorageInfo(StorageInfo storageInfo)
    {
        this.storageInfo = storageInfo;
    }

    public boolean isEncoding()
    {
        return encoding;
    }

    public void setEncoding(boolean encoding)
    {
        this.encoding = encoding;
    }
}
