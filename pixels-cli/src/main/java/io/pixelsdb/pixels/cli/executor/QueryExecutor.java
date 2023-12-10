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
package io.pixelsdb.pixels.cli.executor;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Properties;

import static io.pixelsdb.pixels.cli.Main.executeSQL;

/**
 * @author hank
 * @create 2023-04-16
 */
public class QueryExecutor implements CommandExecutor
{
    @Override
    public void execute(Namespace ns, String command)
    {
        String workload = ns.getString("workload");
        String log = ns.getString("log");
        String cache = ns.getString("drop_cache");

        if (workload != null && log != null)
        {
            ConfigFactory instance = ConfigFactory.Instance();
            Properties properties = new Properties();
            // String user = instance.getProperty("presto.user");
            String password = instance.getProperty("presto.password");
            String ssl = instance.getProperty("presto.ssl");
            String jdbc = instance.getProperty("presto.jdbc.url");

            if (!password.equalsIgnoreCase("null"))
            {
                properties.setProperty("password", password);
            }
            properties.setProperty("SSL", ssl);
            boolean orderedEnabled = Boolean.parseBoolean(instance.getProperty("executor.ordered.layout.enabled"));
            boolean compactEnabled = Boolean.parseBoolean(instance.getProperty("executor.compact.layout.enabled"));
            StringBuilder builder = new StringBuilder()
                    .append("pixels.ordered_path_enabled:").append(orderedEnabled).append(";")
                    .append("pixels.compact_path_enabled:").append(compactEnabled);
            properties.setProperty("sessionProperties", builder.toString());

            try (BufferedReader workloadReader = new BufferedReader(new FileReader(workload));
                 BufferedWriter timeWriter = new BufferedWriter(new FileWriter(log)))
            {
                timeWriter.write("query id,id,duration(ms)\n");
                timeWriter.flush();
                String line;
                int i = 0;
                String defaultUser = null;
                while ((line = workloadReader.readLine()) != null)
                {
                    if (!line.contains("SELECT") && !line.contains("select"))
                    {
                        defaultUser = line;
                        properties.setProperty("user", "pixels-cli-" + defaultUser);
                    } else
                    {
                        if (cache != null)
                        {
                            long start = System.currentTimeMillis();
                            ProcessBuilder processBuilder = new ProcessBuilder(cache);
                            Process process = processBuilder.start();
                            process.waitFor();
                            Thread.sleep(1000);
                            System.out.println("clear cache: " + (System.currentTimeMillis() - start) + "ms\n");
                        }
                        else
                        {
                            Thread.sleep(15 * 1000);
                            System.out.println("wait 15000 ms\n");
                        }

                        long cost = executeSQL(jdbc, properties, line, defaultUser);
                        timeWriter.write(defaultUser + "," + i + "," + cost + "\n");

                        System.out.println(i + "," + cost + "ms");
                        i++;
                        if (i % 10 == 0)
                        {
                            timeWriter.flush();
                            System.out.println(i);
                        }
                    }
                }
                timeWriter.flush();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        } else
        {
            System.out.println("Please input the parameters.");
        }
    }
}
