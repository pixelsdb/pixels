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
package io.pixelsdb.pixels.storage.http.io;

import io.netty.util.AsciiString;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This http content queue orders the input http content by their part id in ascending order.
 * And it ensures that the content being taken (poll, take, peek etc.) has continuous part id.
 * We define the content with a subsequent part id as the <b>legal head</b> of the queue.
 * If the current head content is not a legal head, the take methods will wait.
 * <p/>
 * <b>Note: the content should be retained and released outside this queue.</b>
 *
 * @author hank
 * @create 2025-09-16
 */
public class HttpContentQueue extends AbstractQueue<HttpContent> implements BlockingQueue<HttpContent>
{
    private final PriorityBlockingQueue<HttpContent> queue = new PriorityBlockingQueue<>();

    public static final AsciiString PART_ID = AsciiString.cached("part-id");

    private final AtomicInteger headPartId = new AtomicInteger(0);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notLegalFirst = lock.newCondition();

    @Override
    public Iterator<HttpContent> iterator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size()
    {
        return this.queue.size();
    }

    @Override
    public void put(HttpContent httpContent) throws InterruptedException
    {
        lock.lock();
        try
        {
            this.queue.put(httpContent);
            if (httpContent.getPartId() == this.headPartId.get())
            {
                this.notLegalFirst.signal();
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(HttpContent httpContent, long timeout, TimeUnit unit) throws InterruptedException
    {
        lock.lock();
        try
        {
            boolean ret = this.queue.offer(httpContent, timeout, unit);
            if (httpContent.getPartId() == this.headPartId.get())
            {
                this.notLegalFirst.signal();
            }
            return ret;
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(HttpContent httpContent)
    {
        lock.lock();
        try
        {
            boolean ret = this.queue.offer(httpContent);
            if (httpContent.getPartId() == this.headPartId.get())
            {
                this.notLegalFirst.signal();
            }
            return ret;
        }
        finally
        {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity()
    {
        return this.queue.remainingCapacity();
    }

    @Override
    public int drainTo(Collection<? super HttpContent> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection<? super HttpContent> c, int maxElements)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves and removes the legal head of this queue, waiting if necessary until an element becomes available.
     * @return the legal head of this queue
     * @throws InterruptedException
     */
    @Override
    public HttpContent take() throws InterruptedException
    {
        this.lock.lock();
        try
        {
            while(true)
            {
                int partId = headPartId.get();
                HttpContent content = this.queue.peek();
                if (content != null)
                {
                    if (partId == content.getPartId())
                    {
                        this.headPartId.incrementAndGet();
                        this.queue.remove();
                        return content;
                    }
                    if (partId > content.getPartId())
                    {
                        throw new IllegalStateException("current part id " + partId +
                                " is greater than content part id " + content.getPartId());
                    }
                }
                this.notLegalFirst.await();
            }
        }
        catch (InterruptedException e)
        {
            throw new InterruptedException("interrupted when waiting for the next legal first content to arrive");
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Retrieves and removes the legal head of this queue, waiting up to the specified wait time if necessary for
     * an element to become available.
     * @param timeout how long to wait before giving up, in units of
     *        {@code unit}
     * @param unit a {@code TimeUnit} determining how to interpret the
     *        {@code timeout} parameter
     * @return the legal head of this queue, or null if this queue is empty
     * @throws InterruptedException
     */
    @Override
    public HttpContent poll(long timeout, TimeUnit unit) throws InterruptedException
    {
        if (this.queue.isEmpty())
        {
            return null;
        }
        this.lock.lock();
        try
        {
            while(true)
            {
                int partId = headPartId.get();
                HttpContent content = this.queue.peek();
                if (content != null)
                {
                    if (partId == content.getPartId())
                    {
                        this.headPartId.incrementAndGet();
                        this.queue.remove();
                        return content;
                    }
                    if (partId > content.getPartId())
                    {
                        throw new IllegalStateException("current part id " + partId +
                                " is greater than content part id " + content.getPartId());
                    }
                    this.notLegalFirst.await(timeout, unit);
                }
                else
                {
                    return null;
                }
            }
        }
        catch (InterruptedException e)
        {
            throw new InterruptedException("interrupted when waiting for the next legal first content to arrive");
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Retrieves and removes the legal head of this queue, waiting up to the specified wait time if necessary for
     * an element to become available.
     * @return the legal head of this queue, or null if this queue is empty
     */
    @Override
    public HttpContent poll()
    {
        if (this.queue.isEmpty())
        {
            return null;
        }
        this.lock.lock();
        try
        {
            while(true)
            {
                int partId = headPartId.get();
                HttpContent content = this.queue.peek();
                if (content != null)
                {
                    if (partId == content.getPartId())
                    {
                        this.headPartId.incrementAndGet();
                        this.queue.remove();
                        return content;
                    }
                    if (partId > content.getPartId())
                    {
                        throw new IllegalStateException("current part id " + partId +
                                " is greater than content part id " + content.getPartId());
                    }
                    this.notLegalFirst.await();
                }
                else
                {
                    return null;
                }
            }
        }
        catch (InterruptedException e)
        {
            throw new IllegalStateException("interrupted when waiting for the next legal first content to arrive", e);
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
     * @return the legal head of this queue, or null if this queue is empty
     */
    @Override
    public HttpContent peek()
    {
        if (this.queue.isEmpty())
        {
            return null;
        }
        this.lock.lock();
        try
        {
            while(true)
            {
                int partId = headPartId.get();
                HttpContent content = this.queue.peek();
                if (content != null)
                {
                    if (partId == content.getPartId())
                    {
                        return content;
                    }
                    if (partId > content.getPartId())
                    {
                        throw new IllegalStateException("current part id " + partId +
                                " is greater than content part id " + content.getPartId());
                    }
                    this.notLegalFirst.await();
                }
                else
                {
                    return null;
                }
            }
        }
        catch (InterruptedException e)
        {
            throw new IllegalStateException("interrupted when waiting for the next legal first content to arrive", e);
        }
        finally
        {
            this.lock.unlock();
        }
    }
}
