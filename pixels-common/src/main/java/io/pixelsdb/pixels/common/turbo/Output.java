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
package io.pixelsdb.pixels.common.turbo;

import java.util.Objects;

/**
 * The base class for the output of a cloud function.
 *
 * @author hank
 * @create 2022-06-28
 */
public abstract class Output
{
    private String requestId;
    private boolean successful;
    private String errorMessage;
    private long startTimeMs;
    private int durationMs;
    private int memoryMB;
    private int cumulativeInputCostMs;
    private int cumulativeComputeCostMs;
    private int cumulativeOutputCostMs;
    private int numReadRequests;
    private int numWriteRequests;
    private long totalReadBytes;
    private long totalWriteBytes;

    public Output() {
    }

    public Output(String requestId, boolean successful, String errorMessage, long startTimeMs, int durationMs, int memoryMB, int cumulativeInputCostMs, int cumulativeComputeCostMs, int cumulativeOutputCostMs, int numReadRequests, int numWriteRequests, long totalReadBytes, long totalWriteBytes) {
        this.requestId = requestId;
        this.successful = successful;
        this.errorMessage = errorMessage;
        this.startTimeMs = startTimeMs;
        this.durationMs = durationMs;
        this.memoryMB = memoryMB;
        this.cumulativeInputCostMs = cumulativeInputCostMs;
        this.cumulativeComputeCostMs = cumulativeComputeCostMs;
        this.cumulativeOutputCostMs = cumulativeOutputCostMs;
        this.numReadRequests = numReadRequests;
        this.numWriteRequests = numWriteRequests;
        this.totalReadBytes = totalReadBytes;
        this.totalWriteBytes = totalWriteBytes;
    }

    public Output(Output other) {
        this.requestId = other.requestId;
        this.successful = other.successful;
        this.errorMessage = other.errorMessage;
        this.startTimeMs = other.startTimeMs;
        this.durationMs = other.durationMs;
        this.memoryMB = other.memoryMB;
        this.cumulativeInputCostMs = other.cumulativeInputCostMs;
        this.cumulativeComputeCostMs = other.cumulativeComputeCostMs;
        this.cumulativeOutputCostMs = other.cumulativeOutputCostMs;
        this.numReadRequests = other.numReadRequests;
        this.numWriteRequests = other.numWriteRequests;
        this.totalReadBytes = other.totalReadBytes;
        this.totalWriteBytes = other.totalWriteBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Output output = (Output) o;
        return successful == output.successful && startTimeMs == output.startTimeMs && durationMs == output.durationMs && memoryMB == output.memoryMB && cumulativeInputCostMs == output.cumulativeInputCostMs && cumulativeComputeCostMs == output.cumulativeComputeCostMs && cumulativeOutputCostMs == output.cumulativeOutputCostMs && numReadRequests == output.numReadRequests && numWriteRequests == output.numWriteRequests && totalReadBytes == output.totalReadBytes && totalWriteBytes == output.totalWriteBytes && requestId.equals(output.requestId) && errorMessage.equals(output.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, successful, errorMessage, startTimeMs, durationMs, memoryMB, cumulativeInputCostMs, cumulativeComputeCostMs, cumulativeOutputCostMs, numReadRequests, numWriteRequests, totalReadBytes, totalWriteBytes);
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful)
    {
        this.successful = successful;
    }

    public String getErrorMessage()
    {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage)
    {
        this.errorMessage = errorMessage;
    }

    public long getStartTimeMs()
    {
        return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs)
    {
        this.startTimeMs = startTimeMs;
    }

    public int getDurationMs()
    {
        return durationMs;
    }

    public void setDurationMs(int durationMs)
    {
        this.durationMs = durationMs;
    }

    public int getMemoryMB()
    {
        return memoryMB;
    }

    public void setMemoryMB(int memoryMB)
    {
        this.memoryMB = memoryMB;
    }

    /**
     * @return the GB-ms billed for this function request
     */
    public long getGBMs()
    {
        return Math.round(this.durationMs * (this.memoryMB / 1024.0d));
    }

    public int getCumulativeInputCostMs()
    {
        return cumulativeInputCostMs;
    }

    public void setCumulativeInputCostMs(int cumulativeInputCostMs)
    {
        this.cumulativeInputCostMs = cumulativeInputCostMs;
    }

    public int getCumulativeComputeCostMs()
    {
        return cumulativeComputeCostMs;
    }

    public void setCumulativeComputeCostMs(int cumulativeComputeCostMs)
    {
        this.cumulativeComputeCostMs = cumulativeComputeCostMs;
    }

    public int getCumulativeOutputCostMs()
    {
        return cumulativeOutputCostMs;
    }

    public void setCumulativeOutputCostMs(int cumulativeOutputCostMs)
    {
        this.cumulativeOutputCostMs = cumulativeOutputCostMs;
    }

    public int getNumReadRequests()
    {
        return numReadRequests;
    }

    public void setNumReadRequests(int numReadRequests)
    {
        this.numReadRequests = numReadRequests;
    }

    public int getNumWriteRequests()
    {
        return numWriteRequests;
    }

    public void setNumWriteRequests(int numWriteRequests)
    {
        this.numWriteRequests = numWriteRequests;
    }

    public long getTotalReadBytes()
    {
        return totalReadBytes;
    }

    public void setTotalReadBytes(long totalReadBytes)
    {
        this.totalReadBytes = totalReadBytes;
    }

    public long getTotalWriteBytes()
    {
        return totalWriteBytes;
    }

    public void setTotalWriteBytes(long totalWriteBytes)
    {
        this.totalWriteBytes = totalWriteBytes;
    }
}
