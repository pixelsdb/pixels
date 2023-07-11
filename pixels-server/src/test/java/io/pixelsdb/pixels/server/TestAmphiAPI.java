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
package io.pixelsdb.pixels.server;

import io.pixelsdb.pixels.amphi.AmphiProto;
import io.pixelsdb.pixels.amphi.analyzer.SqlglotExecutor;
import io.pixelsdb.pixels.amphi.AmphiServiceGrpc;
import io.pixelsdb.pixels.common.exception.AmphiException;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author hank
 * @create 2023-04-24
 */
@SpringBootTest(properties = {
        "grpc.server.in-process-name=test", // Enable inProcess server
        "grpc.server.port=-1", // Disable external server
        "grpc.client.mock.address=in-process:test" // Configure the client to connect to the inProcess server
})
@DirtiesContext // Ensures that the grpc-server is properly shutdown after each test, avoiding "port already in use".
public class TestAmphiAPI
{
    @GrpcClient("mock")
    private AmphiServiceGrpc.AmphiServiceBlockingStub amphiService;

    @Test
    @DirtiesContext
    public void testTranspileSqlSimple()
    {
        AmphiProto.TranspileSqlRequest request = AmphiProto.TranspileSqlRequest.newBuilder()
                .setSqlStatement("SELECT EPOCH_MS(1618088028295)")
                .setFromDialect("duckdb")
                .setToDialect("hive")
                .build();

        AmphiProto.TranspileSqlResponse response = amphiService.transpileSql(request);

        assertNotNull(response);
        assertEquals("SELECT FROM_UNIXTIME(1618088028295 / 1000)", response.getSqlTranspiled());
    }

    @Test
    @DirtiesContext
    public void testTranspileSqlTpch()
    {
        String tpchTrinoQuery22 = "SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal FROM CUSTOMER WHERE SUBSTRING(c_phone, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21') AND c_acctbal > (SELECT AVG(c_acctbal) FROM CUSTOMER WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')) AND NOT EXISTS(SELECT * FROM ORDERS WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode NULLS FIRST";
        String tpchDuckdbQuery22 = "SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM (SELECT SUBSTRING(c_phone, 1, 2) AS cntrycode, c_acctbal FROM CUSTOMER WHERE SUBSTRING(c_phone, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21') AND c_acctbal > (SELECT AVG(c_acctbal) FROM CUSTOMER WHERE c_acctbal > 0.00 AND SUBSTRING(c_phone, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')) AND NOT EXISTS(SELECT * FROM ORDERS WHERE o_custkey = c_custkey)) AS custsale GROUP BY cntrycode ORDER BY cntrycode";

        AmphiProto.TranspileSqlRequest request = AmphiProto.TranspileSqlRequest.newBuilder()
                .setSqlStatement(tpchTrinoQuery22)
                .setFromDialect("trino")
                .setToDialect("duckdb")
                .build();

        AmphiProto.TranspileSqlResponse response = amphiService.transpileSql(request);

        assertNotNull(response);
        assertEquals(tpchDuckdbQuery22, response.getSqlTranspiled());
    }

    @Test
    @DirtiesContext
    public void testTranspileSqlParseError()
    {
        String invalidQuery = "SELECT * FROM";

        AmphiProto.TranspileSqlRequest request = AmphiProto.TranspileSqlRequest.newBuilder()
                .setSqlStatement(invalidQuery)
                .setFromDialect("trino")
                .setToDialect("duckdb")
                .build();

        AmphiProto.TranspileSqlResponse response = amphiService.transpileSql(request);

        assertNotNull(response);
        assertEquals(1, response.getHeader().getErrorCode());
        assertEquals("INVALID_ARGUMENT: SQLglot parsing error: Expected table name but got None. Line 1, Col: 10.",
                response.getHeader().getErrorMsg());
    }

    @Test
    @DirtiesContext
    public void testParseColumnFieldsSimple() throws AmphiException, IOException, InterruptedException
    {
        String simpleQuery = "SELECT a, b + 1 AS c FROM d";

        SqlglotExecutor executor = new SqlglotExecutor();
        List<String> columnList = new ArrayList<>();

        columnList = executor.parseColumnFields(simpleQuery);
        assertEquals(2, columnList.size());
        assertEquals(Arrays.asList("a", "b"), columnList);
    }

    @Test
    @DirtiesContext
    public void testParseColumnFieldsTpch() throws AmphiException, IOException, InterruptedException
    {
        String simpleQuery = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice *(1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from LINEITEM where l_shipdate <= date '1998-12-01' - interval '108' day(3) group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus";

        SqlglotExecutor executor = new SqlglotExecutor();
        List<String> columnList = new ArrayList<>();
        columnList = executor.parseColumnFields(simpleQuery);

        Set<String> expectedColumns = new HashSet<>(Arrays.asList("l_returnflag", "l_linestatus", "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_quantity", "l_extendedprice", "l_discount", "l_shipdate", "l_returnflag", "l_linestatus", "l_extendedprice", "l_extendedprice", "l_discount", "l_tax", "l_discount"));
        assertEquals(17, columnList.size());
        assertEquals(expectedColumns, columnList.stream().collect(Collectors.toSet()));
    }


    @Test
    @DirtiesContext
    public void testTrinoQuerySimple()
    {
        String aggregateQuery = "SELECT COUNT(*) AS item_count FROM LINEITEM";

        AmphiProto.TrinoQueryRequest request = AmphiProto.TrinoQueryRequest.newBuilder()
                .setTrinoUrl("ec2-18-218-128-203.us-east-2.compute.amazonaws.com")
                .setTrinoPort(8080)
                .setCatalog("pixels")
                .setSchema("tpch_1g")
                .setSqlQuery(aggregateQuery)
                .build();

        AmphiProto.TrinoQueryResponse response = amphiService.trinoQuery(request);

        assertNotNull(response);
        assertEquals("item_count: 6001215\n", response.getQueryResult());
    }

    @Test
    @DirtiesContext
    public void testTrinoQueryTpch()
    {
        String tpchQuery6 = "select sum(l_extendedprice * l_discount) as revenue from LINEITEM where l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24";

        AmphiProto.TrinoQueryRequest request = AmphiProto.TrinoQueryRequest.newBuilder()
                .setTrinoUrl("ec2-18-218-128-203.us-east-2.compute.amazonaws.com")
                .setTrinoPort(8080)
                .setCatalog("pixels")
                .setSchema("tpch_1g")
                .setSqlQuery(tpchQuery6)
                .build();

        AmphiProto.TrinoQueryResponse response = amphiService.trinoQuery(request);

        assertNotNull(response);
        System.out.println(response.getQueryResult());
        assertEquals("revenue: 123141078.2283\n", response.getQueryResult());
    }
}

