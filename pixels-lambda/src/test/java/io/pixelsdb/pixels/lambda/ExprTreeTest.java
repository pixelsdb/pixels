package io.pixelsdb.pixels.lambda;

import com.google.gson.Gson;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;


public class ExprTreeTest
{
    ExprTree bigintFilter = new ExprTree("aaa", ExprTree.Operator.EQ, "3");
    TypeDescription schema = TypeDescription.fromString("struct<aaa:bigint, bbb:bigint, " +
            "ccc:varchar(79), ddd:date, eee:decimal(15,2), fff:char(1)>");
    VectorizedRowBatch oldRowBatch = schema.createRowBatch();
    ExprTree bigintFilter2 = new ExprTree("bbb", ExprTree.Operator.GT, "2");
    ExprTree exprTree3 = new ExprTree(ExprTree.Operator.AND, bigintFilter, bigintFilter2);
    ExprTree exprTree4 = new ExprTree(ExprTree.Operator.OR, bigintFilter, bigintFilter2);
    ExprTree emptyFilter = new ExprTree();
    ExprTree strFilter = new ExprTree("ccc", ExprTree.Operator.LIKE, "%ant");
    ExprTree strFilter2 = new ExprTree("ccc", ExprTree.Operator.NOT_LIKE, "%ant");
    ExprTree strFilter3 = new ExprTree("fff", ExprTree.Operator.EQ, "P");

    ExprTree statusL = new ExprTree("fff", ExprTree.Operator.EQ, "F");
    ExprTree statusR = new ExprTree("fff", ExprTree.Operator.EQ, "P");
    ExprTree orderStatusFilter = new ExprTree(ExprTree.Operator.OR, statusL, statusR);
    ExprTree twoLevelFilter = new ExprTree(ExprTree.Operator.OR, orderStatusFilter, bigintFilter);


    ArrayList<String> filesToScan = new ArrayList<String>()
    {
        {
            add("pixels-tpch-orders-v-0-order/20220312072707_0.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072714_1.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072720_2.pxl");
            add("pixels-tpch-orders-v-0-order/20220312072727_3.pxl");
        }
    };

    ArrayList<String> cols = new ArrayList<String>()
    {
        {
            add("o_orderkey");
            add("o_custkey");
            add("o_orderstatus");
            add("o_orderdate");
        }
    };

    @Before
    public void initializeOldRowBatch()
    {
        bigintFilter.prepare(schema);
        bigintFilter2.prepare(schema);
        exprTree3.prepare(schema);
        exprTree4.prepare(schema);
        oldRowBatch.size = 5;
        oldRowBatch.cols[0].add(2);
        oldRowBatch.cols[1].add(5);
        oldRowBatch.cols[2].add("kobe");
        oldRowBatch.cols[0].add(3);
        oldRowBatch.cols[1].add(2);
        oldRowBatch.cols[2].add("bryant");
        oldRowBatch.cols[0].add(4);
        oldRowBatch.cols[1].add(2);
        oldRowBatch.cols[2].add("kobe");
        oldRowBatch.cols[0].add(5);
        oldRowBatch.cols[1].add(2);
        oldRowBatch.cols[2].add("bryant");
        oldRowBatch.cols[0].add(3);
        oldRowBatch.cols[1].add(4);
        oldRowBatch.cols[2].add("antisocial");
        oldRowBatch.cols[3].add("1981-01-01"); // these might be off by one day due to time zone shit
        oldRowBatch.cols[3].add("1981-02-01"); // but should be fine because we are comparing two days
        oldRowBatch.cols[3].add("1981-03-01");
        oldRowBatch.cols[3].add("1981-05-01");
        oldRowBatch.cols[3].add("1981-12-01");
        oldRowBatch.cols[4].add(3.23);
        oldRowBatch.cols[5].add("P");
        oldRowBatch.cols[4].add(5.43);
        oldRowBatch.cols[5].add("F");
        oldRowBatch.cols[4].add(2.13);
        oldRowBatch.cols[5].add("P");
        oldRowBatch.cols[4].add(6.00);
        oldRowBatch.cols[5].add("F");
        oldRowBatch.cols[4].add(5.02);
        oldRowBatch.cols[5].add("O");

        System.out.println("oldRowBatch:");
        System.out.println(oldRowBatch);
    }

    @Test
    public void testEvaluateWorks()
    {
        boolean evalRes = bigintFilter.evaluate(oldRowBatch, 1);
        System.out.println(evalRes);
    }

    @Test
    public void testBigIntFilterRowBatch()
    {

        VectorizedRowBatch newRowBatch = bigintFilter.filter(oldRowBatch, schema);
        System.out.println("newRowBatch");
        System.out.println(newRowBatch);
    }

    @Test
    public void testExprTreeFilterRowBatchWithAndOr()
    {
        VectorizedRowBatch newRowBatchAnd = exprTree3.filter(oldRowBatch, schema);
        System.out.println("newRowBatchAnd");
        System.out.println(newRowBatchAnd);
        VectorizedRowBatch newRowBatchOr = exprTree4.filter(oldRowBatch, schema);
        System.out.println("newRowBatchOr");
        System.out.println(newRowBatchOr);
    }

    @Test
    public void testExprTreeToAndFromJson()
    {
        Gson gson = new Gson();
        String jsonStr = gson.toJson(bigintFilter);
        ExprTree tree = gson.fromJson(jsonStr, ExprTree.class);
        VectorizedRowBatch newRowBatch = tree.filter(oldRowBatch, schema);
        System.out.println("newRowBatch");
        System.out.println(newRowBatch);

    }

    @Test
    public void testLambdaEventToAndFromJson()
    {
        Gson gson = new Gson();
//        LambdaEvent lambdaEvent = new LambdaEvent(filesToScan, cols, gson.toJson(bigintFilter));
//        ExprTree filter = lambdaEvent.
//        VectorizedRowBatch newRowBatch = filter.filter(oldRowBatch, schema);
//        System.out.println("newRowBatch");
//        System.out.println(newRowBatch);
    }

    @Test
    public void testEmptyFilter()
    {
        emptyFilter.prepare(schema);
        VectorizedRowBatch newRowBatch = emptyFilter.filter(oldRowBatch, schema);
        System.out.println("newRowBatch");
        System.out.println(newRowBatch);
    }

    @Test
    public void testStrFilter()
    {
        strFilter.prepare(schema);
        VectorizedRowBatch newRowBatch = strFilter.filter(oldRowBatch, schema);
        System.out.println("newRowBatch");
        System.out.println(newRowBatch);

        strFilter2.prepare(schema);
        VectorizedRowBatch newRowBatch2 = strFilter2.filter(oldRowBatch, schema);
        System.out.println("newRowBatch2");
        System.out.println(newRowBatch2);

        strFilter3.prepare(schema);
        VectorizedRowBatch newRowBatch3 = strFilter3.filter(oldRowBatch, schema);
        System.out.println("newRowBatch3");
        System.out.println(newRowBatch3);
    }

    @Test
    public void testDateFilter()
    {
        ExprTree dateFilter = new ExprTree("ddd", ExprTree.Operator.GT, "1981-03-01");
        dateFilter.prepare(schema);
        VectorizedRowBatch newRowBatch = dateFilter.filter(oldRowBatch, schema);
        System.out.println("newRowBatch");
        System.out.println(newRowBatch);
    }

    @Test
    public void testTwoLevelFilter()
    {

        twoLevelFilter.prepare(schema);
        VectorizedRowBatch newRowBatch = twoLevelFilter.filter(oldRowBatch, schema);
        System.out.println("newRowBatch");
        System.out.println(newRowBatch);
    }

    @Test
    public void testDecimalFilter()
    {

        ExprTree dateFilter = new ExprTree("eee", ExprTree.Operator.GE, "5.30");
        dateFilter.prepare(schema);
        VectorizedRowBatch newRowBatch = dateFilter.filter(oldRowBatch, schema);
        System.out.println("newRowBatch");
        System.out.println(newRowBatch);
    }

    // example schema
    //[bigint, bigint, char(1), decimal(15,2), date, char(15), char(15), integer, varchar(79)]

}