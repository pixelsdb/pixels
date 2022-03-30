//TODO add author to all my code

package io.pixelsdb.pixels.lambda;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExprTree
{
    private static final Logger LOGGER = LogManager.getLogger(ExprTree.class);
    ExprTree l = null;
    ExprTree r = null;
    Operator operator; // >, <, = , `like`, and, or
    // leaf operators store 2 operands as field colIndex and constant.

    // leaf operator takes 2 operands and returns a bool. 1st operand is col name, 2nd is a constant of a certain pixels data type (string, int, double, ...). Leaf operators are e.g. >, <, = , `like`
    // non-leaf operators are and, or
    boolean isLeafOp = true;
    String colName = "";
    int colIndex = -1; // TODO maybe this should be final, and so do some other fields

    // if this node is leaf node, this stores type of val. Otherwise it's null
    // this is the type of either a const, or a const read from db
    String constant = ""; // constant is string because originally it's sql input, which is represent as string, but later use colIndex get colType, we can cast this string to the right type , and compare it with the value read from col
    TypeDescription.Category operandType = null;
    Boolean isEmpty = false; // not every scan needs filter. For scans without filter, create an empty filter
    Boolean readyToFilter = false;

    public enum Operator
    {
        GT,  // >
        EQ,  // =
        LT,  // <
        GE, // >=
        LE, // <=
        AND,
        OR,
        LIKE,
        NOT_LIKE
    }

    // create an empty exprTree
    public ExprTree()
    {
        isEmpty = true;
    }

    // construct a leaf node which is an operator  >, <, = , `like` that takes a value read from
    // database and a constant and outputs true/false
    public ExprTree(String colName, Operator leafOperator, String constant)
    {
        this.colName = colName;
        this.operator = leafOperator;
        this.constant = constant;
    }

    // construct a non-leaf node
    public ExprTree(Operator nonLeafOperator, ExprTree l, ExprTree r)
    {
        this.operator = nonLeafOperator;
        this.l = l;
        this.r = r;
        this.isLeafOp = false;
    }

    /**
     * - get col index and operand type (col type and constant type. e.g. for "A" ">" "3", find col A is the 0th col
     * in the rowbatch and is of type long)
     * - this function can't be in the constructor because you need to constuct the filter when you call scan,
     * but when you call scan you don't know the schema. You only know schema after using reader
     *
     * @param schema schema of rowBatches to be filtered by this expression tree
     */
    public void prepare(TypeDescription schema)
    {
        readyToFilter = true;
        if (this.isEmpty)
        {
            return;
        }

        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> colTypes = schema.getChildren();
        for (int i = 0; i < fieldNames.size(); i++)
        {
            if (fieldNames.get(i).equals(colName))
            {
                colIndex = i;
                operandType = colTypes.get(i).getCategory();
                break;
            }
        }

        if (l != null) l.prepare(schema);
        if (l != null) r.prepare(schema);
    }

    /**
     * filter a given row batch
     *
     * @param rowBatch rowBatch to be filtered
     * @param schema   the schema of this rowBatch
     * @return a new filtered rowBatch
     */
    public VectorizedRowBatch filter(VectorizedRowBatch rowBatch, TypeDescription schema)
    {
        assert (readyToFilter) : "not ready to filter: you need to call prepare(schema) first";
        if (this.isEmpty) return rowBatch;

        VectorizedRowBatch newRowBatch = schema.createRowBatch();
        //ColumnVector[] columnVectors = new ColumnVector[rowBatch.numCols];
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

    // todo maybe the content of this function should be inside the for loop of each row directly, i.e. not do a function call for each row
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
            if (operandType == TypeDescription.Category.LONG || operandType == TypeDescription.Category.INT
                    || operandType == TypeDescription.Category.SHORT)
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
            } else if (operandType == TypeDescription.Category.DECIMAL)
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
            } else if (operandType == TypeDescription.Category.DATE)
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
            } else if (operandType == TypeDescription.Category.STRING ||
                    operandType == TypeDescription.Category.CHAR ||
                    operandType == TypeDescription.Category.BINARY ||
                    operandType == TypeDescription.Category.VARCHAR)
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
                LOGGER.error("unknown operand type:" + operandType);
                return false;
            }
        }
    }
//[bigint, bigint, char(1), decimal(15,2), date, char(15), char(15), integer, varchar(79)]

}
