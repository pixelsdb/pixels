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
package io.pixelsdb.pixels.worker.vhive.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.worker.vhive.StreamWorkerCommon;
import one.profiler.AsyncProfiler;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Utils
{
    private static final Storage minio = StreamWorkerCommon.getStorage(Storage.Scheme.minio);
    private static final AsyncProfiler PROFILER = AsyncProfiler.getInstance();
    private static final String EVENT = System.getenv("PROFILING_EVENT");
    private static final String LOG_WORKDIR = System.getenv("LOG_WORKDIR");

    public static void upload(String src, String dest) throws IOException
    {
        // write to the log file
        dest = String.format("%s/%s", LOG_WORKDIR, dest);
        // String dir = dest.substring(0, dest.lastIndexOf("/"));
        // minio.mkdirs(dir);

        try (FileInputStream iStream = new FileInputStream(src);
             DataOutputStream oStream = minio.create(dest, false, Constants.S3_BUFFER_SIZE)) {

            byte[] buffer = new byte[4096];
            int bytesRead;

            while ((bytesRead = iStream.read(buffer)) != -1) {
                oStream.write(buffer, 0, bytesRead);
            }

            oStream.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void dump(String filename, Object... contents) throws IOException
    {
        FileOutputStream outputStream = new FileOutputStream(filename);
        JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(Arrays.asList(contents));
        String output = jsonArray.toString(SerializerFeature.PrettyFormat, SerializerFeature.DisableCircularReferenceDetect);
        outputStream.write(output.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
    }

    public static void startProfile(String filename) throws IOException
    {
        PROFILER.execute(String.format("start,jfr,threads,total,event=%s,file=%s", EVENT, filename));
    }

    public static void stopProfile(String filename) throws IOException
    {
        PROFILER.execute(String.format("stop,file=%s", filename));
    }
}
