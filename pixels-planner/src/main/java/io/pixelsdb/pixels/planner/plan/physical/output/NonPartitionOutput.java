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
package io.pixelsdb.pixels.planner.plan.physical.output;

import io.pixelsdb.pixels.common.turbo.Output;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The output format for serverless operators.
 * @author hank
 * @create 2022-04-11
 */
public abstract class NonPartitionOutput extends Output
{
    /**
     * The path of the result files. No need to contain endpoint information.
     */
    private List<String> outputs = new ArrayList<>();

    /**
     * The number of row groups in each result files.
     */
    private List<Integer> rowGroupNums = new ArrayList<>();

    /**
     * Default constructor for jackson.
     */
    public NonPartitionOutput() {
        super();
    }

    public NonPartitionOutput(List<String> outputs, List<Integer> rowGroupNums) {
        super();
        this.outputs = outputs;
        this.rowGroupNums = rowGroupNums;
    }

    public NonPartitionOutput(Output output, List<String> outputs, List<Integer> rowGroupNums) {
        super(output);
        this.outputs = outputs;
        this.rowGroupNums = rowGroupNums;
    }

    public NonPartitionOutput(NonPartitionOutput other) {
        super(other);
        this.outputs = other.outputs;
        this.rowGroupNums = other.rowGroupNums;
    }

    public List<String> getOutputs() {
        return outputs;
    }

    public void setOutputs(ArrayList<String> outputs) {
        this.outputs = outputs;
    }

    public List<Integer> getRowGroupNums() {
        return rowGroupNums;
    }

    public void setRowGroupNums(ArrayList<Integer> rowGroupNums) {
        this.rowGroupNums = rowGroupNums;
    }

    public synchronized void addOutput(String output, int rowGroupNum) {
        this.outputs.add(output);
        this.rowGroupNums.add(rowGroupNum);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        NonPartitionOutput that = (NonPartitionOutput) o;
        return outputs.equals(that.outputs) && rowGroupNums.equals(that.rowGroupNums);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), outputs, rowGroupNums);
    }
}
