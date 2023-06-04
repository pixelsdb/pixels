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

/**
 * @author hank
 * @create 2023-05-24
 */
public class GetResultResponse
{
    private int errorCode;
    private String errorMessage;
    private int[] columnPrintSizes;
    private String[] columnNames;
    private String[][] rows;
    private double latencyMs;
    private double costCents;

    /**
     * Default constructor for Jackson.
     */
    public GetResultResponse() { }

    public GetResultResponse(int errorCode, String errorMessage, int[] columnPrintSizes,
                             String[] columnNames, String[][] rows, double latencyMs, double costCents)
    {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.columnPrintSizes = columnPrintSizes;
        this.columnNames = columnNames;
        this.rows = rows;
        this.latencyMs = latencyMs;
        this.costCents = costCents;
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

    public int[] getColumnPrintSizes()
    {
        return columnPrintSizes;
    }

    public void setColumnPrintSizes(int[] columnPrintSizes)
    {
        this.columnPrintSizes = columnPrintSizes;
    }

    public String[] getColumnNames()
    {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames)
    {
        this.columnNames = columnNames;
    }

    public String[][] getRows()
    {
        return rows;
    }

    public void setRows(String[][] rows)
    {
        this.rows = rows;
    }

    public double getLatencyMs()
    {
        return latencyMs;
    }

    public void setLatencyMs(double latencyMs)
    {
        this.latencyMs = latencyMs;
    }

    public double getCostCents()
    {
        return costCents;
    }

    public void setCostCents(double costCents)
    {
        this.costCents = costCents;
    }
}
