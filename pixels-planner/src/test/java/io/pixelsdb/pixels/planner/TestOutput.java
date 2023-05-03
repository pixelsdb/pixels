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
package io.pixelsdb.pixels.planner;

import com.google.gson.Gson;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.junit.Test;

/**
 * @author hank
 * @create 2022-04-11
 */
public class TestOutput
{
    @Test
    public void testEncodeScanOutput()
    {
        ScanOutput scanOutput = new ScanOutput();
        scanOutput.addOutput("pixels-test/0.out", 1);
        scanOutput.addOutput("pixels-test/1.out", 1);
        scanOutput.addOutput("pixels-test/2.out", 1);
        scanOutput.addOutput("pixels-test/3.out", 1);
        Gson gson = new Gson();
        String json = gson.toJson(scanOutput);
        assert json != null && !json.isEmpty();
        System.out.println(json);
    }
}
