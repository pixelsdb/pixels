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

import io.pixelsdb.pixels.common.utils.Constants;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * A decoder for a sequence of bytes encoded by {@link RunLenByteEncoder}.
 * A control byte is read before each run with positive values 0 to 127 meaning 3 to 130
 * repetitions. If the byte is -1 to -128, 1 to 128 literal byte values follow.
 *
 * @author guodong
 * @author hank
 */
public class RunLenByteDecoder extends Decoder
{
    private final InputStream inputStream;
    private final byte[] literals = new byte[Constants.BYTE_RLE_MAX_LITERAL_SIZE];
    private int numLiterals = 0;
    private int used = 0;
    private boolean repeat = false;

    public RunLenByteDecoder(InputStream inputStream)
    {
        this.inputStream = inputStream;
    }

    public byte next() throws IOException
    {
        if (used == numLiterals)
        {
            readValues();
        }
        if (repeat)
        {
            used += 1;
            return literals[0];
        }
        else
        {
            return literals[used++];
        }
    }

    @Override
    public boolean hasNext() throws IOException
    {
        return used != numLiterals || inputStream.available() > 0;
    }

    @Override
    public void close() throws IOException
    {
        if (inputStream != null)
        {
            inputStream.close();
        }
    }

    private void readValues() throws IOException
    {
        int nextByte = inputStream.read();
        if (nextByte == -1)
        {
            throw new EOFException("Read past end of buffer RLE byte");
        }

        int control = (byte) nextByte;
        int runLength;
        if (control >= 0)
        {
            // repeat: control 0..127 means 3..130 repetitions
            int val = inputStream.read();
            if (val == -1)
            {
                throw new EOFException("Reading RLE byte got EOF");
            }
            literals[0] = (byte) val;
            runLength = control + Constants.RLE_MIN_REPEAT;
        }
        else
        {
            // literal: control -1..-128 means 1..128 literal bytes
            runLength = -control;
            int bytes = 0;
            while (bytes < runLength)
            {
                int result = inputStream.read(literals, bytes, runLength - bytes);
                if (result <= 0)
                {
                    throw new EOFException("Reading RLE byte literal got EOF");
                }
                bytes += result;
            }
        }

        // Publish a run only after its complete payload has been read.
        repeat = control >= 0;
        used = 0;
        numLiterals = runLength;
    }
}
