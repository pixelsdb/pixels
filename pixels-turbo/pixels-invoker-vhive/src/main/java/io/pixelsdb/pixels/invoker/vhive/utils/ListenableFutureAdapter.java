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
package io.pixelsdb.pixels.invoker.vhive.utils;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.CompletableFuture;

public class ListenableFutureAdapter<T>
{
    private final ListenableFuture<T> listenableFuture;
    private final CompletableFuture<T> completableFuture;

    public ListenableFutureAdapter(ListenableFuture<T> listenableFuture)
    {
        this.listenableFuture = listenableFuture;
        this.completableFuture = new CompletableFuture<T>()
        {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning)
            {
                boolean cancelled = listenableFuture.cancel(mayInterruptIfRunning);
                super.cancel(cancelled);
                return cancelled;
            }
        };

        Futures.addCallback(this.listenableFuture, new FutureCallback<T>()
        {
            @Override
            public void onSuccess(T result)
            {
                completableFuture.complete(result);
            }

            @Override
            public void onFailure(Throwable ex)
            {
                completableFuture.completeExceptionally(ex);
            }
        }, MoreExecutors.directExecutor());
    }

    public static final <T> CompletableFuture<T> toCompletable(ListenableFuture<T> listenableFuture)
    {
        ListenableFutureAdapter<T> listenableFutureAdapter = new ListenableFutureAdapter<>(listenableFuture);
        return listenableFutureAdapter.getCompletableFuture();
    }

    public CompletableFuture<T> getCompletableFuture()
    {
        return completableFuture;
    }

}
