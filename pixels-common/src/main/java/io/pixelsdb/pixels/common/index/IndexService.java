/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.index.IndexProto;
import java.util.List;

public interface IndexService
{
    /**
     * Allocate a batch of continuous row ids for the primary index on a table.
     * These row ids are to be put into the primary index by the client (e.g., retina or sink)
     *
     * @param tableId   the table id of the table
     * @param numRowIds the number of row ids to allocate
     * @return the allocated row ids
     * @throws IndexException if failed to allocate row ids
     */
    IndexProto.RowIdBatch allocateRowIdBatch(long tableId, int numRowIds) throws IndexException;

    /**
     * Lookup a unique index.
     * @param key the index key
     * @return the row location or null if the index entry is not found
     */
    IndexProto.RowLocation lookupUniqueIndex(IndexProto.IndexKey key) throws IndexException;

    /**
     * Lookup a non-unique index.
     * @param key the index key
     * @return the row locations or null if the index entry is not found
     */
    List<IndexProto.RowLocation> lookupNonUniqueIndex(IndexProto.IndexKey key) throws IndexException;

    /**
     * Put an index entry into the primary index.
     * @param entry the index entry
     * @return true on success
     */
    boolean putPrimaryIndexEntry(IndexProto.PrimaryIndexEntry entry) throws IndexException;

    /**
     * Put an index entry into the secondary index.
     * @param entry the index entry
     * @return true on success
     */
    boolean putSecondaryIndexEntry(IndexProto.SecondaryIndexEntry entry) throws IndexException;

    /**
     * Put a batch of index entries into the primary index.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param entries the index entries
     * @return true on success
     */
    boolean putPrimaryIndexEntries(long tableId, long indexId,
                                   List<IndexProto.PrimaryIndexEntry> entries) throws IndexException;

    /**
     * Put a batch of index entries into the secondary index.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param entries the index entries
     * @return true on success
     */
    boolean putSecondaryIndexEntries(long tableId, long indexId,
                                     List<IndexProto.SecondaryIndexEntry> entries) throws IndexException;

    /**
     * Delete an entry from the primary index. The deleted index entry is marked as deleted using a tombstone.
     * @param key the index key
     * @return the row location of the deleted index entry
     * @throws IndexException if no existing entry to delete
     */
    IndexProto.RowLocation deletePrimaryIndexEntry(IndexProto.IndexKey key) throws IndexException;

    /**
     * Delete entry(ies) from the secondary index. Each deleted index entry is marked as deleted using a tombstone.
     * @param key the index key
     * @return the row id(s) of the deleted index entry(ies)
     * @throws IndexException if no existing entry(ies) to delete
     */
    List<Long> deleteSecondaryIndexEntry(IndexProto.IndexKey key) throws IndexException;

    /**
     * Delete entries from the primary index. Each deleted index entry is marked as deleted using a tombstone.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param keys the keys of the entries to delete
     * @return the row locations of the deleted index entries
     * @throws IndexException if no existing entry(ies) to delete
     */
    List<IndexProto.RowLocation> deletePrimaryIndexEntries(long tableId, long indexId,
                                                           List<IndexProto.IndexKey> keys) throws IndexException;

    /**
     * Delete entries from the secondary index. Each deleted index entry is marked as deleted using a tombstone.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param keys the keys of the entries to delete
     * @return the row ids of the deleted index entries
     * @throws IndexException if no existing entry(ies) to delete
     */
    List<Long> deleteSecondaryIndexEntries(long tableId, long indexId,
                                           List<IndexProto.IndexKey> keys) throws IndexException;

    /**
     * Update the entry of a primary index.
     * @param indexEntry the index entry to update
     * @return the previous row location of the index entry
     * @throws IndexException if no existing entry to update
     */
    IndexProto.RowLocation updatePrimaryIndexEntry(IndexProto.PrimaryIndexEntry indexEntry) throws IndexException;

    /**
     * Update the entry of a secondary index.
     * @param indexEntry the index entry to update
     * @return the previous row id(s) of the index entry
     * @throws IndexException if no existing entry(ies) to update
     */
    List<Long> updateSecondaryIndexEntry(IndexProto.SecondaryIndexEntry indexEntry) throws IndexException;

    /**
     * Update the entries of a primary index.
     * @param tableId the table id of the primary index
     * @param indexId the index id of the primary index
     * @param indexEntries the index entries to update
     * @return the previous row locations of the index entries
     * @throws IndexException if no existing entry(ies) to update
     */
    List<IndexProto.RowLocation> updatePrimaryIndexEntries(long tableId, long indexId,
                                                           List<IndexProto.PrimaryIndexEntry> indexEntries) throws IndexException;

    /**
     * Update the entries of a secondary index.
     * @param tableId the table id of the secondary index
     * @param indexId the index id of the secondary index
     * @param indexEntries the index entries to update
     * @return the previous row ids of the index entries
     * @throws IndexException if no existing entry(ies) to update
     */
    List<Long> updateSecondaryIndexEntries(long tableId, long indexId,
                                           List<IndexProto.SecondaryIndexEntry> indexEntries) throws IndexException;

    /**
     * Purge (remove) the index entries of an index permanently. This should only be done asynchronously by the garbage
     * collection process.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param indexKeys the index keys of the index entries
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    boolean purgeIndexEntries(long tableId, long indexId,
                              List<IndexProto.IndexKey> indexKeys, boolean isPrimary) throws IndexException;

    /**
     * Flush the index entries of an index corresponding to a buffered Pixels data file.
     * In Pixels, the index entries corresponding to a write-buffered data file (usually stored in the write buffer)
     * may be buffered in memory by the index server. This method tells to index server to flush such buffered index
     * entries for a write-buffered data file. This methods should be called by retina when flushing a write buffer.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param fileId the file id of the write buffer
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    boolean flushIndexEntriesOfFile(long tableId, long indexId,
                                    long fileId, boolean isPrimary) throws IndexException;

    /**
     * Open an index in the index server. This method is optional and is used to pre-warm an index instance in
     * the index server. Even without calling this method, an index will be opened on its first access.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    boolean openIndex(long tableId, long indexId, boolean isPrimary) throws IndexException;

    /**
     * Close an index in the index server.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    boolean closeIndex(long tableId, long indexId, boolean isPrimary) throws IndexException;

    /**
     * Close an index and remove its persistent storage content in the index server.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    boolean removeIndex(long tableId, long indexId, boolean isPrimary) throws IndexException;
}

