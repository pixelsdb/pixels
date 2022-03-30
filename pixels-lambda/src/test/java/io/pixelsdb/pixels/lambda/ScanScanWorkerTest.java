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

import org.junit.Assert;
import org.junit.Test;

public class ScanScanWorkerTest
{
    ScanWorker worker = new ScanWorker();
    //'{ "bucketName":"pixels-tpch-customer-v-0-order", "fileName": "20220213140252_0.pxl" }'
    String[] cols = {"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"};
    ExprTree filter = new ExprTree("o_orderkey", ExprTree.Operator.GT, "3000");

    @Test
    public void testScanFileCanGrabColumnWithCorrectType() {
        String result =  worker.scanFile("pixels-tpch-orders-v-0-order/20220306043322_0.pxl", 1024, cols, filter, "aaaaid123asdjjkhj88");
        String expected = "success";
        Assert.assertEquals(result, expected);
    }
}