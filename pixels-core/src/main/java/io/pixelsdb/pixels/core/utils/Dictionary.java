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
package io.pixelsdb.pixels.core.utils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The dictionary used for string dictionary encoding.
 * @author hank
 * @date 8/15/22
 */
public interface Dictionary
{
    int add(String key);

    int add(byte[] key);

    int add(byte[] key, int offset, int length);

    /**
     * @return the number of items in the dictionary
     */
    int size();

    void clear();

    void visit(Visitor visitor) throws IOException;

    /**
     * The interface for dictionary visitors.
     */
    interface Visitor
    {
        /**
         * Called once for each item in the dictionary.
         * @param context the information about each item
         * @throws IOException
         */
        void visit(VisitorContext context) throws IOException;
    }

    /**
     * The information about each item in the dictionary.
     */
    interface VisitorContext
    {
        /**
         * Get the position, i.e., id of the key in the dictionary.
         * @return the number returned by {@link #add(byte[], int, int)}.
         */
        int getKeyPosition();

        /**
         * Write the bytes for the string to the given output stream.
         * @param out the stream to write to.
         * @throws IOException
         */
        void writeBytes(OutputStream out) throws IOException;

        /**
         * @return the key's length in bytes
         */
        int getLength();
    }
}
