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
package io.pixelsdb.pixels.common.index.service;

import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.ResolvedPrimary;
import io.pixelsdb.pixels.common.index.RollbackEntry;
import io.pixelsdb.pixels.index.IndexProto;

import java.util.List;
import java.util.Optional;

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
     * @return the row location, or null if the key is missing or maps to an orphan
     */
    IndexProto.RowLocation lookupUniqueIndex(IndexProto.IndexKey key, IndexOption indexOption) throws IndexException;

    /**
     * Lookup a non-unique index.
     * @param key the index key
     * @return the row locations or null if the index entry is not found
     */
    List<IndexProto.RowLocation> lookupNonUniqueIndex(IndexProto.IndexKey key, IndexOption indexOption) throws IndexException;

    /**
     * Put an index entry into the primary index.
     * @param entry the index entry
     * @return true on success
     */
    boolean putPrimaryIndexEntry(IndexProto.PrimaryIndexEntry entry, IndexOption indexOption) throws IndexException;

    /**
     * Put an index entry into the secondary index.
     * @param entry the index entry
     * @return true on success
     */
    boolean putSecondaryIndexEntry(IndexProto.SecondaryIndexEntry entry, IndexOption indexOption) throws IndexException;

    /**
     * Put a batch of index entries into the primary index.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param entries the index entries
     * @return true on success
     */
    boolean putPrimaryIndexEntries(long tableId, long indexId,
                                   List<IndexProto.PrimaryIndexEntry> entries, IndexOption indexOption) throws IndexException;

    /**
     * Put a batch of index entries into the secondary index.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param entries the index entries
     * @return true on success
     */
    boolean putSecondaryIndexEntries(long tableId, long indexId,
                                     List<IndexProto.SecondaryIndexEntry> entries, IndexOption indexOption) throws IndexException;

    /**
     * Delete an entry from the primary index. The deleted index entry is marked as deleted using a tombstone.
     * Crash-unsafe; prefer {@link #resolvePrimary} + {@link #deletePrimaryIndexEntriesOnly}.
     * @param key the index key
     * @return the row location of the deleted index entry
     * @throws IndexException if no existing entry to delete
     */
    IndexProto.RowLocation deletePrimaryIndexEntry(IndexProto.IndexKey key, IndexOption indexOption) throws IndexException;

    /**
     * Delete entry(ies) from the secondary index. Each deleted index entry is marked as deleted using a tombstone.
     * @param key the index key
     * @return the row id(s) of the deleted index entry(ies)
     * @throws IndexException if no existing entry(ies) to delete
     */
    List<Long> deleteSecondaryIndexEntry(IndexProto.IndexKey key, IndexOption indexOption) throws IndexException;

    /**
     * Delete entries from the primary index. Each deleted index entry is marked as deleted using a tombstone.
     * Crash-unsafe; prefer {@link #resolvePrimary} + {@link #deletePrimaryIndexEntriesOnly}.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param keys the keys of the entries to delete
     * @param indexOption the extra option
     * @return the row locations of the deleted index entries
     * @throws IndexException if no existing entry(ies) to delete
     */
    List<IndexProto.RowLocation> deletePrimaryIndexEntries(long tableId, long indexId,
                                                           List<IndexProto.IndexKey> keys, IndexOption indexOption) throws IndexException;

    /**
     * Delete entries from the secondary index. Each deleted index entry is marked as deleted using a tombstone.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param keys the keys of the entries to delete
     * @return the row ids of the deleted index entries
     * @throws IndexException if no existing entry(ies) to delete
     */
    List<Long> deleteSecondaryIndexEntries(long tableId, long indexId,
                                           List<IndexProto.IndexKey> keys, IndexOption indexOption) throws IndexException;

    /**
     * Update the entry of a primary index.
     * Crash-unsafe; prefer DELETE + INSERT.
     * @param indexEntry the index entry to update
     * @return the previous row location of the index entry
     * @throws IndexException if no existing entry to update
     */
    IndexProto.RowLocation updatePrimaryIndexEntry(IndexProto.PrimaryIndexEntry indexEntry, IndexOption indexOption) throws IndexException;

    /**
     * Update the entry of a secondary index.
     * @param indexEntry the index entry to update
     * @return the previous row id(s) of the index entry
     * @throws IndexException if no existing entry(ies) to update
     */
    List<Long> updateSecondaryIndexEntry(IndexProto.SecondaryIndexEntry indexEntry, IndexOption indexOption) throws IndexException;

    /**
     * Update the entries of a primary index.
     * Crash-unsafe; prefer DELETE + INSERT.
     * @param tableId the table id of the primary index
     * @param indexId the index id of the primary index
     * @param indexEntries the index entries to update
     * @return the previous row locations of the index entries
     * @throws IndexException if no existing entry(ies) to update
     */
    List<IndexProto.RowLocation> updatePrimaryIndexEntries(long tableId, long indexId,
                                                           List<IndexProto.PrimaryIndexEntry> indexEntries, IndexOption indexOption) throws IndexException;

    /**
     * Update the entries of a secondary index.
     * @param tableId the table id of the secondary index
     * @param indexId the index id of the secondary index
     * @param indexEntries the index entries to update
     * @return the previous row ids of the index entries
     * @throws IndexException if no existing entry(ies) to update
     */
    List<Long> updateSecondaryIndexEntries(long tableId, long indexId,
                                           List<IndexProto.SecondaryIndexEntry> indexEntries, IndexOption indexOption) throws IndexException;

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
                              List<IndexProto.IndexKey> indexKeys, boolean isPrimary, IndexOption indexOption) throws IndexException;

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
                                    long fileId, boolean isPrimary, IndexOption indexOption) throws IndexException;

    /**
     * Open an index in the index server. This method is optional and is used to pre-warm an index instance in
     * the index server. Even without calling this method, an index will be opened on its first access.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    boolean openIndex(long tableId, long indexId, boolean isPrimary, IndexOption indexOption) throws IndexException;

    /**
     * Close an index in the index server.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    boolean closeIndex(long tableId, long indexId, boolean isPrimary, IndexOption option) throws IndexException;

    /**
     * Close an index and remove its persistent storage content in the index server.
     * @param tableId the table id of the index
     * @param indexId the index id of the index
     * @param isPrimary true if the index is a primary index
     * @return true on success
     */
    boolean removeIndex(long tableId, long indexId, boolean isPrimary, IndexOption option) throws IndexException;

    // ==================================================================================
    // Staged primary-index APIs. Default implementations throw UnsupportedOperationException; 
    // LocalIndexService provides the in-process implementation.
    // ==================================================================================

    /**
     * Resolve a batch of primary index keys to {@link ResolvedPrimary} (rowId + RowLocation),
     * positionally aligned with keys. Returns Optional.empty() for keys
     * that are missing, tombstoned, orphan in MainIndex, or filtered out by the
     * baseline visible file set; throws on backend error.
     *
     * @param tableId the table id of the primary index
     * @param indexId the index id of the primary index
     * @param keys the primary index keys to resolve
     * @param indexOption optional index option
     * @return positional list of resolved primaries
     * @throws IndexException on backend error
     */
    default List<Optional<ResolvedPrimary>> resolvePrimary(long tableId, long indexId,
            List<IndexProto.IndexKey> keys, IndexOption indexOption) throws IndexException
    {
        throw new UnsupportedOperationException(
                "resolvePrimary is not supported by this IndexService scheme");
    }

    /**
     * Write rowId -> RowLocation entries into the main index.
     *
     * @param tableId the table id of the main index
     * @param entries the entries to persist
     * @throws IndexException on backend error
     */
    default void putMainIndexEntriesOnly(long tableId,
            List<IndexProto.PrimaryIndexEntry> entries) throws IndexException
    {
        throw new UnsupportedOperationException(
                "putMainIndexEntriesOnly is not supported by this IndexService scheme");
    }

    /**
     * Write IndexKey -> rowId entries into the primary single point index.
     *
     * @param tableId the table id of the primary index
     * @param indexId the index id of the primary index
     * @param entries the entries to persist
     * @param indexOption optional index option
     * @throws IndexException on backend error
     */
    default void putPrimaryIndexEntriesOnly(long tableId, long indexId,
            List<IndexProto.PrimaryIndexEntry> entries, IndexOption indexOption) throws IndexException
    {
        throw new UnsupportedOperationException(
                "putPrimaryIndexEntriesOnly is not supported by this IndexService scheme");
    }

    /**
     * Delete primary index entries for keys already resolved by {@link #resolvePrimary}.
     * Repeating on an already-deleted key is a no-op.
     *
     * @param tableId the table id of the primary index
     * @param indexId the index id of the primary index
     * @param resolvedKeys the keys to delete
     * @param indexOption optional index option
     * @throws IndexException on backend error
     */
    default void deletePrimaryIndexEntriesOnly(long tableId, long indexId,
            List<IndexProto.IndexKey> resolvedKeys, IndexOption indexOption) throws IndexException
    {
        throw new UnsupportedOperationException(
                "deletePrimaryIndexEntriesOnly is not supported by this IndexService scheme");
    }

    /**
     * Update primary index entries to the new IndexKey -> rowId mapping;
     * does not look up the previous rowId.
     *
     * @param tableId the table id of the primary index
     * @param indexId the index id of the primary index
     * @param entries the new IndexKey -> rowId mappings
     * @param indexOption optional index option
     * @throws IndexException on backend error
     */
    default void updatePrimaryIndexEntriesOnly(long tableId, long indexId,
            List<IndexProto.PrimaryIndexEntry> entries, IndexOption indexOption) throws IndexException
    {
        throw new UnsupportedOperationException(
                "updatePrimaryIndexEntriesOnly is not supported by this IndexService scheme");
    }

    /**
     * Restore primary index entries to oldRowId where the current pointer
     * still equals newRowId; skip otherwise. Intended for single-threaded
     * rollback windows and does not require atomic conditional update from the backend.
     *
     * @param tableId the table id of the primary index
     * @param indexId the index id of the primary index
     * @param entries rollback entries describing each oldRowId -> newRowId transition
     * @param indexOption optional index option
     * @throws IndexException on backend error
     */
    default void restorePrimaryIndexEntries(long tableId, long indexId,
            List<RollbackEntry> entries, IndexOption indexOption) throws IndexException
    {
        throw new UnsupportedOperationException(
                "restorePrimaryIndexEntries is not supported by this IndexService scheme");
    }
}

