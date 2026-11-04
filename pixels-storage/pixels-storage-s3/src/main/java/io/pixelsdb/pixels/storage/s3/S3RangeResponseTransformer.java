/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.storage.s3;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/** A known-length range needs one destination array, not a growing response accumulator. */
final class S3RangeResponseTransformer implements AsyncResponseTransformer<GetObjectResponse, ResponseBytes<GetObjectResponse>>
{
    private final int length;
    private final String rangePrefix;
    private volatile Attempt attempt;

    S3RangeResponseTransformer(long offset, int length)
    {
        this.length = length;
        this.rangePrefix = "bytes " + offset + "-" + (offset + length - 1) + "/";
    }

    @Override
    public CompletableFuture<ResponseBytes<GetObjectResponse>> prepare()
    {
        // SDK retries get independent buffers; late body callbacks retain their own attempt.
        attempt = new Attempt();
        return attempt.result;
    }

    @Override
    public void onResponse(GetObjectResponse response)
    {
        Attempt current = attempt;
        if ((response.contentLength() != null && response.contentLength() != length) ||
                (response.contentRange() != null && !response.contentRange().startsWith(rangePrefix)))
        {
            current.result.completeExceptionally(new IOException("Unexpected S3 range response: " + response));
        }
        else { current.response = response; }
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) { publisher.subscribe(attempt); }

    @Override
    public void exceptionOccurred(Throwable failure) { attempt.result.completeExceptionally(failure); }

    private final class Attempt implements Subscriber<ByteBuffer>
    {
        final CompletableFuture<ResponseBytes<GetObjectResponse>> result = new CompletableFuture<>();
        volatile GetObjectResponse response;
        byte[] bytes = new byte[length];
        int position;
        Subscription subscription;

        Attempt()
        {
            result.whenComplete((value, failure) -> {
                Subscription pending;
                synchronized (this) { bytes = null; pending = subscription; subscription = null; }
                if (failure != null && pending != null) { pending.cancel(); }
            });
        }

        @Override
        public void onSubscribe(Subscription incoming)
        {
            boolean accepted;
            synchronized (this)
            {
                accepted = subscription == null && !result.isDone();
                if (accepted) { subscription = incoming; }
            }
            if (accepted) { incoming.request(Long.MAX_VALUE); }
            else { incoming.cancel(); }
        }

        @Override
        public void onNext(ByteBuffer buffer)
        {
            boolean oversized;
            synchronized (this)
            {
                if (result.isDone()) { return; }
                int count = buffer.remaining();
                oversized = count > length - position;
                if (!oversized)
                {
                    buffer.duplicate().get(bytes, position, count);
                    position += count;
                }
            }
            if (oversized) { result.completeExceptionally(new IOException("S3 range body exceeds " + length + " bytes")); }
        }

        @Override
        public void onError(Throwable failure) { result.completeExceptionally(failure); }

        @Override
        public void onComplete()
        {
            ResponseBytes<GetObjectResponse> value;
            synchronized (this)
            {
                if (result.isDone()) { return; }
                value = position == length && response != null ? ResponseBytes.fromByteArrayUnsafe(response, bytes) : null;
            }
            if (value == null) { result.completeExceptionally(new IOException("Incomplete S3 range body: " + position + "/" + length)); }
            else { result.complete(value); }
        }
    }
}
