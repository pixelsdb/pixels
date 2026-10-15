/*
 * Copyright 2026 PixelsDB.
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.ingest;

import java.util.Objects;

/**
 * One writer's ordered substream in one table/shard. Neither a business key nor
 * a hostname is an identity component. An RPC retry preserves this identity.
 */
public final class MutationStreamId
{
    public enum Kind
    {
        APPEND_ROWS(1), DELETE_ROWS(2);

        private final int code;

        Kind(int code)
        {
            this.code = code;
        }

        public int getCode()
        {
            return code;
        }

        public static Kind fromCode(int code)
        {
            for (Kind kind : values())
            {
                if (kind.code == code)
                {
                    return kind;
                }
            }
            throw new IllegalArgumentException("Unknown mutation kind: " + code);
        }
    }

    private final long transactionId;
    private final long statementId;
    private final long writerId;
    private final long tableId;
    private final int shardId;
    private final Kind kind;

    public MutationStreamId(long transactionId, long statementId, long writerId, long tableId,
                            int shardId, Kind kind)
    {
        if (transactionId < 0 || statementId <= 0 || writerId < 0 || tableId < 0 || shardId < 0)
        {
            throw new IllegalArgumentException(
                    "Stream identifiers must be non-negative and statementId must be positive");
        }
        this.transactionId = transactionId;
        this.statementId = statementId;
        this.writerId = writerId;
        this.tableId = tableId;
        this.shardId = shardId;
        this.kind = Objects.requireNonNull(kind, "kind");
    }

    public long getTransactionId() { return transactionId; }
    public long getStatementId() { return statementId; }
    public long getWriterId() { return writerId; }
    public long getTableId() { return tableId; }
    public int getShardId() { return shardId; }
    public Kind getKind() { return kind; }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) { return true; }
        if (!(other instanceof MutationStreamId)) { return false; }
        MutationStreamId that = (MutationStreamId) other;
        return transactionId == that.transactionId && statementId == that.statementId && writerId == that.writerId
                && tableId == that.tableId && shardId == that.shardId && kind == that.kind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(transactionId, statementId, writerId, tableId, shardId, kind);
    }

    @Override
    public String toString()
    {
        return transactionId + ":" + statementId + ":" + writerId + ":" + tableId + ":" + shardId + ":" + kind;
    }
}
