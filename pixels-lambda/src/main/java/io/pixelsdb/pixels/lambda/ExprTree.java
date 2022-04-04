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
package io.pixelsdb.pixels.lambda;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExprTree
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExprTree.class);

    ExprTree l = null;
    ExprTree r = null;
    Operator operator;

    /**
     * Not every scan needs filter. For scans without filter, create an empty filter.
     */
    Boolean isEmpty = false;
    Boolean readyToFilter = false;

    /**
     * Leaf operators are e.g. >, <, = , `like`. Whereas non-leaf operators are 'and', 'or'.
     * Leaf operator takes 2 operands and returns a bool.
     * The 1st operand is col name, and the 2nd is a constant of a value of the
     * corresponding data type (string, int, double, ...).
     */
    boolean isLeafOp = true;

    // The following members are only used in a leaf operator.

    /**
     * The column name in the leaf node.
     * e.g., for the predicate 'orderkey = 100', the colName is 'orderkey'.
     */
    String colName = "";
    int colIndex = -1; // TODO maybe this should be final, and so do some other fields

    /**
     * The constant value in the leaf node.
     * It is from the sql input, and can be cast to the real type of the corresponding column.
     * e.g., for the predicate 'orderkey = 100', the constant is '100'.
     */
    String constant = "";

    TypeDescription.Category colType = null;

    public enum Operator
    {
        GT, // >
        EQ, // =
        LT, // <
        GE, // >=
        LE, // <=
        LIKE,
        NOT_LIKE,
        AND,
        OR
    }

    /**
     * Create an empty exprTree.
     */
    public ExprTree()
    {
        isEmpty = true;
    }

    /**
     * Create a leaf node that represents a basic predicate of the type >, <, = , `like` etc.
     * For example 'orderkey=100'.
     * @param colName the column name in the predicate.
     * @param leafOperator the operator type of the predicate.
     * @param constant the constant condition in the predicate.
     */
    public ExprTree(String colName, Operator leafOperator, String constant)
    {
        this.colName = colName;
        this.operator = leafOperator;
        this.constant = constant;
    }

    /**
     * Create a non-leaf node of the type 'and' or 'or'.
     * @param nonLeafOperator the operator type.
     * @param l the left child.
     * @param r the right child.
     */
    public ExprTree(Operator nonLeafOperator, ExprTree l, ExprTree r)
    {
        this.operator = nonLeafOperator;
        this.l = l;
        this.r = r;
        this.isLeafOp = false;
    }

    /**
     * Initialize the colIndex and colType. e.g. for "A > 3", if A of the type long and
     * is the 0th col in the result row batch, then colIndex would be 0 and colType would be long.
     *
     * @param resultSchema the schema of the result row batch to be filtered by this expression tree.
     */
    public void prepare(TypeDescription resultSchema)
    {
        readyToFilter = true;
        if (this.isEmpty)
        {
            return;
        }

        List<String> fieldNames = resultSchema.getFieldNames();
        List<TypeDescription> colTypes = resultSchema.getChildren();
        for (int i = 0; i < fieldNames.size(); i++)
        {
            if (fieldNames.get(i).equals(colName))
            {
                colIndex = i;
                colType = colTypes.get(i).getCategory();
                break;
            }
        }

        if (l != null) l.prepare(resultSchema);
        if (l != null) r.prepare(resultSchema);
    }

    /**
     * Filter a given row batch
     *
     * @param rowBatch the rowBatch to be filtered.
     * @param schema   the schema of this rowBatch.
     * @return the filtered rowBatch.
     */
    public VectorizedRowBatch filter(VectorizedRowBatch rowBatch, TypeDescription schema)
    {
        assert (readyToFilter) : "not ready to filter: you need to call prepare(schema) first";
        if (this.isEmpty) return rowBatch;

        VectorizedRowBatch newRowBatch = schema.createRowBatch();
        // loop through row batch row by row
        for (int rowId = 0; rowId < rowBatch.size; rowId++)
        {
            if (evaluate(rowBatch, rowId))
            {
                newRowBatch.size += 1;
                // this row makes predicate true, add this row to filteredRowBatch
                for (int colId = 0; colId < rowBatch.numCols; colId++)
                {
                    newRowBatch.cols[colId].setElement(newRowBatch.size - 1, rowId, rowBatch.cols[colId]);
                }
            }
        }
        return newRowBatch;
    }

    public boolean evaluate(VectorizedRowBatch rowBatch, int rowId)
    {
        if (!isLeafOp)
        {
            if (operator == Operator.AND)
            {
                return (l.evaluate(rowBatch, rowId) && r.evaluate(rowBatch, rowId));
            } else
            {
                // this is an OR
                return (l.evaluate(rowBatch, rowId) || r.evaluate(rowBatch, rowId));
            }
        } else
        {
            // this is a leaf node. First read the val from col, then compare with a const
            // fixme: these "if"s  look redundant but avoid function calls and casting
            if (colType == TypeDescription.Category.LONG || colType == TypeDescription.Category.INT
                    || colType == TypeDescription.Category.SHORT)
            {
                LongColumnVector colVec = (LongColumnVector) rowBatch.cols[colIndex];
                long valRead = colVec.vector[rowId];
                //TODO parse constant to long and compare with the read value
                long longConst = Long.parseLong(constant);
                if (operator == Operator.GT) return valRead > longConst;
                if (operator == Operator.EQ) return valRead == longConst;
                if (operator == Operator.LT) return valRead < longConst;
                if (operator == Operator.GE) return valRead >= longConst;
                if (operator == Operator.LE) return valRead <= longConst;
                LOGGER.error("unknown leaf operator: " + operator);
                return false;
            } else if (colType == TypeDescription.Category.DECIMAL)
            {
                DecimalColumnVector colVec = (DecimalColumnVector) rowBatch.cols[colIndex];
                double valRead = 1.0 * colVec.vector[rowId] / Math.pow(10, colVec.scale);
                double doubleConst = Double.parseDouble(constant);
//                System.out.println("valRead:"+valRead);
//                System.out.println("doubleConst:"+doubleConst);
                if (operator == Operator.GT) return valRead > doubleConst;
                if (operator == Operator.EQ) return valRead == doubleConst;
                if (operator == Operator.LT) return valRead < doubleConst;
                if (operator == Operator.GE) return valRead >= doubleConst;
                if (operator == Operator.LE) return valRead <= doubleConst;
                LOGGER.error("unknown leaf operator: " + operator);
                return false;
            } else if (colType == TypeDescription.Category.DATE)
            {
                DateColumnVector colVec = (DateColumnVector) rowBatch.cols[colIndex];
                Date valRead = colVec.asScratchDate(rowId);
                Date dateConst = Date.valueOf(constant);
                if (operator == Operator.GT) return valRead.compareTo(dateConst) > 0;
                if (operator == Operator.EQ) return valRead.compareTo(dateConst) == 0;
                if (operator == Operator.LT) return valRead.compareTo(dateConst) < 0;
                if (operator == Operator.GE) return valRead.compareTo(dateConst) >= 0;
                if (operator == Operator.LE) return valRead.compareTo(dateConst) <= 0;
                LOGGER.error("unknown leaf operator: " + operator);
                return false;
            } else if (colType == TypeDescription.Category.STRING ||
                    colType == TypeDescription.Category.CHAR ||
                    colType == TypeDescription.Category.BINARY ||
                    colType == TypeDescription.Category.VARCHAR)
            {
                BinaryColumnVector colVec = (BinaryColumnVector) rowBatch.cols[colIndex];
//                StringBuilder buf = new StringBuilder();
//                colVec.stringifyValue(buf, rowId);
                String valRead = new String(colVec.vector[rowId], colVec.start[rowId], colVec.lens[rowId]);
//                System.out.println("valRead:"+valRead);
                String strConst = constant;
//                System.out.println("strConst: " + strConst);
                if (operator == Operator.GT) return valRead.compareTo(strConst) > 0;
                if (operator == Operator.EQ) return valRead.compareTo(strConst) == 0;
                if (operator == Operator.LT) return valRead.compareTo(strConst) < 0;
                if (operator == Operator.GE) return valRead.compareTo(strConst) >= 0;
                if (operator == Operator.LE) return valRead.compareTo(strConst) <= 0;
                String javaRegex = strConst.replace("%", ".*");
                //System.out.println("javaRegex: " + javaRegex);
                Pattern pattern = Pattern.compile(javaRegex);
                Matcher matcher = pattern.matcher(valRead);
                if (operator == Operator.LIKE)
                {
                    //The percent sign represents zero, one or multiple characters
                    return matcher.find();
                }
                if (operator == Operator.NOT_LIKE)
                {
                    return !matcher.find();
                }
                LOGGER.error("unknown leaf operator: " + operator);
                return false;
            } else
            {
                LOGGER.error("unknown operand type:" + colType);
                return false;
            }
        }
    }
}
