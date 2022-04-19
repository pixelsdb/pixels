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
package io.pixelsdb.pixels.core.lambda;

import java.util.ArrayList;

/**
 * The input format for ScanWorker.
 * @author hank
 * Created at: 11/04/2022
 */
public class ScanInput
{
    /**
     * The unique id of the query.
     */
    private long queryId;
    /**
     * The information of the input files to be scanned.
     * rgLength in each input is the number of row groups to be scanned from each file.
     */
    private ArrayList<InputInfo> inputs;
    /**
     * The number of row groups to be scanned in each query split.
     */
    private int splitSize;
    /**
     * The description of the output folder where the scan results are written into.
     */
    private OutputInfo output;
    /**
     * The name of the columns to scan.
     */
    private String[] cols;
    /**
     * The json string of the filter (i.e., predicates) to be used in scan.
     */
    private String filter;

    /**
     * Default constructor for Jackson.
     */
    public ScanInput() { }

    public ScanInput(long queryId, ArrayList<InputInfo> inputs, int splitSize,
                     OutputInfo output, String[] cols, String filter)
    {
        this.queryId = queryId;
        this.inputs = inputs;
        this.splitSize = splitSize;
        this.output = output;
        this.cols = cols;
        this.filter = filter;
    }

    public long getQueryId()
    {
        return queryId;
    }

    public void setQueryId(long queryId)
    {
        this.queryId = queryId;
    }

    public ArrayList<InputInfo> getInputs()
    {
        return inputs;
    }

    public void setInputs(ArrayList<InputInfo> inputs)
    {
        this.inputs = inputs;
    }

    public int getSplitSize()
    {
        return splitSize;
    }

    public void setSplitSize(int splitSize)
    {
        this.splitSize = splitSize;
    }

    public OutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(OutputInfo output)
    {
        this.output = output;
    }

    public String[] getCols()
    {
        return cols;
    }

    public void setCols(String[] cols)
    {
        this.cols = cols;
    }

    public String getFilter()
    {
        return filter;
    }

    public void setFilter(String filter)
    {
        this.filter = filter;
    }

    public static class InputInfo
    {
        private String filePath;
        private int rgStart;
        private int rgLength;

        /**
         * Default constructor for Jackson.
         */
        public InputInfo() { }

        public InputInfo(String filePath, int rgStart, int rgLength)
        {
            this.filePath = filePath;
            this.rgStart = rgStart;
            this.rgLength = rgLength;
        }

        public String getFilePath()
        {
            return filePath;
        }

        public void setFilePath(String filePath)
        {
            this.filePath = filePath;
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

    public static class OutputInfo
    {
        /**
         * The folder path, i.e., bucket_name/folder, where the scan results are written into.
         */
        private String folder;
        /**
         * The endpoint of the output storage, e.g., http://hostname:port.
         */
        private String endpoint;
        /**
         * The access key of the output storage.
         */
        private String accessKey;
        /**
         * The secret key of the output storage.
         */
        private String secretKey;

        private boolean encoding;

        /**
         * Default constructor for Jackson.
         */
        public OutputInfo() { }

        public OutputInfo(String folder, String endpoint, String accessKey,
                          String secretKey, boolean encoding)
        {
            this.folder = folder;
            this.endpoint = endpoint;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.encoding = encoding;
        }

        public String getFolder()
        {
            return folder;
        }

        public void setFolder(String folder)
        {
            this.folder = folder;
        }

        public String getEndpoint()
        {
            return endpoint;
        }

        public void setEndpoint(String endpoint)
        {
            this.endpoint = endpoint;
        }

        public String getAccessKey()
        {
            return accessKey;
        }

        public void setAccessKey(String accessKey)
        {
            this.accessKey = accessKey;
        }

        public String getSecretKey()
        {
            return secretKey;
        }

        public void setSecretKey(String secretKey)
        {
            this.secretKey = secretKey;
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
}
