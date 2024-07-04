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
package io.pixelsdb.pixels.cli;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.pixelsdb.pixels.core.PixelsProto;
import org.junit.Test;

/**
 * @author hank
 * @create 2023-12-06
 */
public class TestPrintProtobuf
{
    @Test
    public void test() throws InvalidProtocolBufferException
    {
        PixelsProto.PostScript postScript = PixelsProto.PostScript.newBuilder().setVersion(1)
                .setCompression(PixelsProto.CompressionKind.LZ4).setCompressionBlockSize(3).build();
        String json = JsonFormat.printer().print(postScript);
        System.out.println(json);
    }
}
