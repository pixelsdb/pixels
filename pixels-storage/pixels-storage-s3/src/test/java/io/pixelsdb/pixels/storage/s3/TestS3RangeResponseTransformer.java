/* Copyright 2026 PixelsDB. Licensed under the AGPL-3.0 license. */
package io.pixelsdb.pixels.storage.s3;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class TestS3RangeResponseTransformer
{
    private static final class Body implements SdkPublisher<ByteBuffer>, Subscription
    {
        Subscriber<? super ByteBuffer> subscriber;
        boolean cancelled;
        long requested;
        public void subscribe(Subscriber<? super ByteBuffer> subscriber) { this.subscriber = subscriber; subscriber.onSubscribe(this); }
        public void request(long count) { requested += count; }
        public void cancel() { cancelled = true; }
    }

    @Test
    public void arbitraryBuffersAreCombinedWithoutChangingTheirCursors() throws Exception
    {
        S3RangeResponseTransformer transformer = new S3RangeResponseTransformer(10, 4);
        CompletableFuture<ResponseBytes<GetObjectResponse>> result = transformer.prepare();
        transformer.onResponse(GetObjectResponse.builder().contentLength(4L).contentRange("bytes 10-13/20").build());
        Body body = new Body(); transformer.onStream(body);
        ByteBuffer first = ByteBuffer.wrap(new byte[]{9, 1, 2, 9}); first.position(1); first.limit(3);
        ByteBuffer second = ByteBuffer.allocateDirect(4); second.put(new byte[]{9, 3, 4, 9}); second.position(1); second.limit(3);
        body.subscriber.onNext(first.asReadOnlyBuffer()); body.subscriber.onNext(ByteBuffer.allocate(0)); body.subscriber.onNext(second);
        assertFalse(result.isDone()); body.subscriber.onComplete();
        assertArrayEquals(new byte[]{1, 2, 3, 4}, result.get().asByteArrayUnsafe());
        assertEquals(1, first.position()); assertEquals(1, second.position()); assertFalse(body.cancelled);
    }

    @Test
    public void invalidHeadersAndShortOrLongBodiesCannotSucceed() throws Exception
    {
        for (int mode = 0; mode < 4; mode++)
        {
            S3RangeResponseTransformer transformer = new S3RangeResponseTransformer(0, 4);
            CompletableFuture<?> result = transformer.prepare();
            GetObjectResponse.Builder response = GetObjectResponse.builder().contentLength(mode == 0 ? 5L : 4L);
            if (mode == 1) { response.contentRange("bytes 1-4/20"); }
            transformer.onResponse(response.build());
            Body body = new Body(); transformer.onStream(body);
            if (mode > 1) { body.subscriber.onNext(ByteBuffer.allocate(mode == 2 ? 3 : 5)); body.subscriber.onComplete(); }
            assertTrue(assertThrows(ExecutionException.class, result::get).getCause() instanceof IOException);
            assertTrue(body.cancelled);
        }
    }

    @Test
    public void retriesDoNotShareBytesOrLateBodyCallbacks() throws Exception
    {
        S3RangeResponseTransformer transformer = new S3RangeResponseTransformer(0, 2);
        CompletableFuture<?> failed = transformer.prepare();
        transformer.onResponse(GetObjectResponse.builder().contentLength(2L).build());
        Body old = new Body(); transformer.onStream(old); old.subscriber.onNext(ByteBuffer.wrap(new byte[]{9}));
        transformer.exceptionOccurred(new IOException("first attempt"));
        assertTrue(failed.isCompletedExceptionally()); assertTrue(old.cancelled);
        CompletableFuture<ResponseBytes<GetObjectResponse>> success = transformer.prepare();
        transformer.onResponse(GetObjectResponse.builder().contentLength(2L).build());
        Body next = new Body(); transformer.onStream(next);
        old.subscriber.onNext(ByteBuffer.wrap(new byte[]{9})); old.subscriber.onComplete(); old.subscriber.onError(new IOException("late"));
        next.subscriber.onNext(ByteBuffer.wrap(new byte[]{1, 2})); next.subscriber.onComplete();
        assertArrayEquals(new byte[]{1, 2}, success.get().asByteArrayUnsafe());
    }

    @Test
    public void cancellationBeforeAndAfterSubscriptionCancelsTheBody()
    {
        for (boolean before : new boolean[]{true, false})
        {
            S3RangeResponseTransformer transformer = new S3RangeResponseTransformer(0, 2);
            CompletableFuture<?> result = transformer.prepare(); Body body = new Body();
            if (before) { result.cancel(true); }
            transformer.onStream(body);
            if (!before) { result.cancel(true); }
            assertTrue(body.cancelled); if (before) { assertEquals(0, body.requested); }
            body.subscriber.onNext(ByteBuffer.allocate(2)); body.subscriber.onComplete();
            assertTrue(result.isCancelled());
        }
    }
}
