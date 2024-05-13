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

import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.server.ExecutionHint;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hank
 * @create 2023-05-24
 */
public class GetQueryResultResponse
{
    private int errorCode;
    private String errorMessage;
    private int[] columnPrintSizes;
    private String[] columnNames;
    private String[][] rows;
    /**
     * The time in ms the query waits in the queue for execution.
     */
    private double pendingTimeMs;
    /**
     * The time the query really executes.
     */
    private double executionTimeMs;
    /**
     * The amount of money in cents really spent by the query.
     */
    private double costCents = 0;
    /**
     * The amount of money in cents billed according to our price model.
     */
    private double billedCents = 0;

    // Issue #649: Breakdown costs into vmCost and cfCost
    /**
     * The amount of money in cents really spent in vm by the query.
     */
    private double vmCostCents = -1;
    /**
     * The amount of money in cents really spent in cf by the query.
     */
    private double cfCostCents = -1;
    /**
     * The execution hint of query.
     */
    private ExecutionHint executionHint;

    /**
     * Default constructor for Jackson.
     */
    public GetQueryResultResponse() { }

    /**
     * Construct a response with error. Error code and error message must be set to tell the error details.
     * @param errorCode the error code
     * @param errorMessage the error message
     */
    public GetQueryResultResponse(int errorCode, String errorMessage)
    {
        checkArgument(errorCode != ErrorCode.SUCCESS,
                "this constructor is only used to create a response with error");
        checkArgument(errorMessage != null && !errorMessage.isEmpty(),
                "error message can not be null or empty");
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public GetQueryResultResponse(int errorCode, String errorMessage, ExecutionHint executionHint, int[] columnPrintSizes,
                                  String[] columnNames, String[][] rows, double pendingTimeMs,
                                  double executionTimeMs)
    {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.executionHint = executionHint;
        this.columnPrintSizes = columnPrintSizes;
        this.columnNames = columnNames;
        this.rows = rows;
        this.pendingTimeMs = pendingTimeMs;
        this.executionTimeMs = executionTimeMs;
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

    public ExecutionHint getExecutionHint() { return executionHint; }

    public void setExecutionHint(ExecutionHint executionHint) { this.executionHint = executionHint; }

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

    public double getPendingTimeMs()
    {
        return pendingTimeMs;
    }

    public void setPendingTimeMs(double pendingTimeMs)
    {
        this.pendingTimeMs = pendingTimeMs;
    }

    public double getExecutionTimeMs()
    {
        return executionTimeMs;
    }

    public void setExecutionTimeMs(double executionTimeMs)
    {
        this.executionTimeMs = executionTimeMs;
    }

    public double getCostCents()
    {
        return costCents;
    }

    public void setCostCents(double costCents)
    {
        this.costCents = costCents;
    }

    public void addCostCents(double costCents) { this.costCents += costCents; }

    public double getBilledCents()
    {
        return billedCents;
    }

    public void setBilledCents(double billedCents)
    {
        this.billedCents = billedCents;
    }

    public void setVmCostCents(double vmCostCents)
    {
        this.vmCostCents = vmCostCents;
    }

    public double getVmCostCents()
    {
        return vmCostCents;
    }

    public void setCfCostCents(double cfCostCents)
    {
        this.cfCostCents = cfCostCents;
    }

    public double getCfCostCents()
    {
        return cfCostCents;
    }

    public boolean hasValidVmCostCents()
    {
        return this.vmCostCents >= 0;
    }
}
