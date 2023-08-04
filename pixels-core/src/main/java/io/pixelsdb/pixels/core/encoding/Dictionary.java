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
package io.pixelsdb.pixels.core.encoding;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The dictionary used for string dictionary encoding.
 * @author hank
 * @create 2022-08-15
 */
public interface Dictionary
{
    /**
     * Add a key (i.e., a string) into the dictionary.
     * @param key the byte content of the key.
     * @return the position (i.e., encoded id) of the key.
     */
    int add(String key);

    /**
     * Add a key (i.e., a string) into the dictionary.
     * Note that to reduce memory allocation and copying, key might be stored in the dictionary
     * without copying, therefore the content in key should not be overwritten after being added.
     * @param key the byte content of the key.
     * @return the position (i.e., encoded id) of the key.
     */
    int add(byte[] key);

    /**
     * Add a key (i.e., a string) into the dictionary.
     * Note that to reduce memory allocation and copying, key might be stored in the dictionary
     * without copying, therefore the content in key should not be overwritten after being added.
     * @param key the byte content of the key.
     * @param offset the starting offset of the key in the byte array.
     * @param length the length in bytes of the key.
     * @return the position (i.e., encoded id) of the key.
     */
    int add(byte[] key, int offset, int length);

    /**
     * @return the number of items in the dictionary
     */
    int size();

    void clear();

    /**
     * This method prepares a {@link VisitorContext} instance for each item (key) in this dictionary,
     * and calls {@link Visitor#visit(VisitorContext)} to visit the item.
     * <p>
     * The items <b>MUST</b> be visited in the ascending order of the item's key position, i.e.,
     * the encoded id of the item in this dictionary.
     * The visitor can write (serialize) the dictionary item to an output stream.
     * </p>
     * @param visitor the visitor that is going to serialize the dictionary to an output stream.
     * @throws IOException
     */
    void visit(Visitor visitor) throws IOException;

    /**
     * The interface for dictionary visitors.
     */
    interface Visitor
    {
        /**
         * Called exact once for each item in the dictionary.
         * @param context the information about each item.
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
         * Write the key to the given output stream.
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
