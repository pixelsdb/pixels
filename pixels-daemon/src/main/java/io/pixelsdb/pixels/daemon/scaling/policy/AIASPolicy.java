/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.daemon.scaling.policy;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class AIASPolicy extends Policy
{
    private static final Logger log = LogManager.getLogger(BasicPolicy.class);

    private void scaling(double[] cpuTimes, double[] memUsages)
    {
         /* the unit of cpuTimes is 5mins = the forecast interval
            spl = [10*1000, 60*1000, 5*60*1000, 10*60*1000] #10s 1min 5min 10min
            memspl = [G, 8*G, 16*G, 32*G, 64*G, 128*G] #1G 8G 16G 32G 64G 128G */
        int WorkerNum1 = (int)Math.ceil((Arrays.stream(cpuTimes).sum()-cpuTimes[cpuTimes.length-1])/8);
        int WorkerNum2 = (int)Math.ceil((Arrays.stream(memUsages).sum()-memUsages[memUsages.length-1])/32);
        int WorkerNum = Math.max(WorkerNum1, WorkerNum2);
        scalingManager.expendTo(WorkerNum);
        log.info("INFO: expend to "+ WorkerNum1 + " or " + WorkerNum2);
    }

    public boolean transDump(long timestamp)
    {
        String host = ConfigFactory.Instance().getProperty("trans.server.host");
        int port = Integer.parseInt(ConfigFactory.Instance().getProperty("trans.server.port"));
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host,port)
                .usePlaintext().build();
        TransServiceGrpc.TransServiceBlockingStub stub = TransServiceGrpc.newBlockingStub(channel);
        TransProto.DumpTransRequest request = TransProto.DumpTransRequest.newBuilder().setTimestamp(timestamp).build();
        TransProto.DumpTransResponse response = stub.dumpTrans(request);
        channel.shutdownNow();
        return true;
    }

    @Override
    public void doAutoScaling()
    {
        try
        {
            ConfigFactory config = ConfigFactory.Instance();
            long timestamp = System.currentTimeMillis();
            timestamp /= 7*24*60*60*1000;
            transDump(timestamp);
            // forecast according to history data
            String[] cmd = {config.getProperty("python.env.path"),
                            config.getProperty("forecast.code.path"),
                            config.getProperty("pixels.historyData.dir")+ timestamp + ".csv",
                            config.getProperty("pixels.historyData.dir")+ (timestamp-1) + ".csv",
                            config.getProperty("cpuspl"),
                            config.getProperty("memspl")
                            };
            Process proc = Runtime.getRuntime().exec(cmd);
            proc.waitFor();
            BufferedReader br1 = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            BufferedReader br2 = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

            String line;
            while ((line = br2.readLine()) != null) {
                log.error(line);
            }
            line = br1.readLine();
            double[] cpuTimes = Arrays.stream(line.split(" "))
                    .mapToDouble(Double::parseDouble)
                    .toArray();
            line = br1.readLine();
            double[] memUsages = Arrays.stream(line.split(" "))
                    .mapToDouble(Double::parseDouble)
                    .toArray();
            scaling(cpuTimes, memUsages);
        } catch (IOException e)
        {
            e.printStackTrace();
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
