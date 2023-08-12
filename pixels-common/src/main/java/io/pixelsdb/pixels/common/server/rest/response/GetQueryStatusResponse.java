/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.common.server.rest.response;

import io.pixelsdb.pixels.common.server.QueryStatus;

import java.util.Map;

/**
 * @author hank
 * @create 2023-07-23
 */
public class GetQueryStatusResponse
{
    private int errorCode;
    private String errorMessage;
    private Map<String, QueryStatus> queryStatuses;

    /**
     * Default constructor for Jackson.
     */
    public GetQueryStatusResponse() { }

    public GetQueryStatusResponse(int errorCode, String errorMessage, Map<String, QueryStatus> queryStatuses)
    {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.queryStatuses = queryStatuses;
    }

    public int getErrorCode()
    {
        return errorCode;
    }

    public void setErrorCode(int errorCode)
    {
        this.errorCode = errorCode;
    }

    public String getErrorMessage()
    {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage)
    {
        this.errorMessage = errorMessage;
    }

    public Map<String, QueryStatus> getQueryStatuses()
    {
        return queryStatuses;
    }

    public void setQueryStatuses(Map<String, QueryStatus> queryStatuses)
    {
        this.queryStatuses = queryStatuses;
    }
}
