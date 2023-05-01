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

import net.devh.boot.grpc.client.inject.GrpcClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

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
}
