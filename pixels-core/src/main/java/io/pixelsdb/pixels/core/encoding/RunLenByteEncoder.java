/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core.encoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A encoder for a sequence of bytes.
 * A control byte is written before each run with positive values 0 to 127 meaning 2 to 129 repetitions.
 * If the bytes is -1 to -128, 1 to 128 literal byte values follow.
 */
public class RunLenByteEncoder
        extends Encoder
{
    private static final int MIN_REPEAT_SIZE = 3;
    private static final int MAX_LITERAL_SIZE = 128;
    private static final int MAX_REPEAT_SIZE = 127 + MIN_REPEAT_SIZE;

    private final ByteArrayOutputStream output;
    private final byte[] literals = new byte[MAX_LITERAL_SIZE];
    private int numLiterals = 0;
    private boolean repeat = false;
    private int tailRunLength = 0;

    public RunLenByteEncoder()
    {
        this.output = new ByteArrayOutputStream();
    }

    @Override
    public byte[] encode(byte[] values)
            throws IOException
    {
        for (byte v : values)
        {
            write(v);
        }
        flush();
        byte[] result = output.toByteArray();
        output.reset();
        return result;
    }

    @Override
    public byte[] encode(byte[] values, long offset, long length)
            throws IOException
    {
        for (int i = 0; i < length; i++)
        {
            write(values[i + (int) offset]);
        }
        flush();
        byte[] result = output.toByteArray();
        output.reset();
        return result;
    }

    @Override
    public void close()
            throws IOException
    {
        output.close();
    }

    private void writeValues()
    {
        if (numLiterals != 0)
        {
            if (repeat)
            {
                output.write(numLiterals - MIN_REPEAT_SIZE);
                output.write(literals, 0, 1);
            }
            else
            {
                output.write(-numLiterals);
                output.write(literals, 0, numLiterals);
            }
            repeat = false;
            tailRunLength = 0;
            numLiterals = 0;
        }
    }

    private void flush()
            throws IOException
    {
        writeValues();
        output.flush();
    }

    private void write(byte value)
    {
        if (numLiterals == 0)
        {
            literals[numLiterals++] = value;
            tailRunLength = 1;
        }
        else if (repeat)
        {
            if (value == literals[0])
            {
                numLiterals += 1;
                if (numLiterals == MAX_REPEAT_SIZE)
                {
                    writeValues();
                }
            }
            else
            {
                writeValues();
                literals[numLiterals++] = value;
                tailRunLength = 1;
            }
        }
        else
        {
            if (value == literals[numLiterals - 1])
            {
                tailRunLength += 1;
            }
            else
            {
                tailRunLength = 1;
            }
            if (tailRunLength == MIN_REPEAT_SIZE)
            {
                if (numLiterals + 1 == MIN_REPEAT_SIZE)
                {
                    repeat = true;
                    numLiterals += 1;
                }
                else
                {
                    numLiterals -= MIN_REPEAT_SIZE - 1;
                    writeValues();
                    literals[0] = value;
                    repeat = true;
                    numLiterals = MIN_REPEAT_SIZE;
                }
            }
            else
            {
                literals[numLiterals++] = value;
                if (numLiterals == MAX_LITERAL_SIZE)
                {
                    writeValues();
                }
            }
        }
    }
}
