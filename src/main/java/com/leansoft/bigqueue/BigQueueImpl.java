package com.leansoft.bigqueue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.leansoft.bigqueue.page.IMappedPage;
import com.leansoft.bigqueue.page.IMappedPageFactory;
import com.leansoft.bigqueue.page.MappedPageFactoryImpl;


/**
 * A big, fast and persistent queue implementation.
 * <p/>
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 *
 * @author bulldog
 */
public class BigQueueImpl implements IBigQueue {

    final IBigArray innerArray;

    // 2 ^ 3 = 8
    final static int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    final static int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    
    // 2 ^ 3 = 8
 	final static int PID_DATA_ITEM_LENGTH_BITS = 3;
 	// size in bytes of pid data page.
 	final static int PID_DATA_PAGE_SIZE = 1 << PID_DATA_ITEM_LENGTH_BITS;
 	
    // only use the first page
    static final long QUEUE_FRONT_PAGE_INDEX = 0;
    // only use the first page.
 	static final long PID_DATA_PAGE_INDEX = 0;

    // folder name for queue front index page
    final static String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";
    
    // folder name for pid data page.
 	final static String PID_DATA_PAGE_FOLDER = "pid";

    // front index of the big queue,
    final AtomicLong queueFrontIndex = new AtomicLong();
    
    final AtomicLong queueTempFrontIndex = new AtomicLong();

    // factory for queue front index page management(acquire, release, cache)
    IMappedPageFactory queueFrontIndexPageFactory;
    
    // factory for pid page management(acquire, release, cache)
 	IMappedPageFactory pidPageFactory; 

    // locks for queue front write management
    final Lock queueFrontWriteLock = new ReentrantLock();
    
    // lock for dequeueFuture access
    private final Object futureLock = new Object();
    private SettableFuture<byte[]> dequeueFuture;
    private SettableFuture<byte[]> peekFuture;

    /**
     * A big, fast and persistent queue implementation,
     * use default back data page size, see {@link BigArrayImpl#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName) throws IOException {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big, fast and persistent queue implementation.
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName, int pageSize) throws IOException {
        this(queueDir, queueName, pageSize, Integer.MAX_VALUE);
    }
    
    /**
     * A big, fast and persistent queue implementation.
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
     * @param maxpages  max allowed pages for big queue.
     * @throws IOException exception throws if there is any IO error during queue initialization
     */
    public BigQueueImpl(String queueDir, String queueName, int pageSize, int maxAllowedPages) throws IOException {
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize, maxAllowedPages);

    	this.pidPageFactory = new MappedPageFactoryImpl(PID_DATA_PAGE_SIZE, 
    			 ((BigArrayImpl) innerArray).getArrayDirectory() + PID_DATA_PAGE_FOLDER, 
				10 * 1000/*does not matter*/);
		if (!this.pidPageFactory.tryLock(PID_DATA_PAGE_INDEX)) {
			throw new IOException(String.format("File array built on directory {} is locked by other process.", ((BigArrayImpl) innerArray).getArrayDirectory()));
		}
		
        // the ttl does not matter here since queue front index page is always cached
        this.queueFrontIndexPageFactory = new MappedPageFactoryImpl(QUEUE_FRONT_INDEX_PAGE_SIZE,
                ((BigArrayImpl) innerArray).getArrayDirectory() + QUEUE_FRONT_INDEX_PAGE_FOLDER,
                10 * 1000/*does not matter*/);
        IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
        long front = queueFrontIndexBuffer.getLong();
        queueFrontIndex.set(front);
        queueTempFrontIndex.set(front);
    }
    
    @Override
    public boolean isEmpty() {
        return this.queueFrontIndex.get() == this.innerArray.getHeadIndex();
    }

    @Override
    public void enqueue(byte[] data) throws IOException {
        this.innerArray.append(data);

        this.completeFutures();
    }
    
    @Override
    public byte[] dequeue() throws IOException {
        long queueFrontIndex = -1L;
        try {
            queueFrontWriteLock.lock();
            if (this.queueTempFrontIndex.get() == this.innerArray.getHeadIndex()) {
                return null;
            }
            queueFrontIndex = this.queueTempFrontIndex.get();
            byte[] data = this.innerArray.get(queueFrontIndex);
            long nextQueueFrontIndex = queueFrontIndex;
            if (nextQueueFrontIndex == Long.MAX_VALUE) {
                nextQueueFrontIndex = 0L; // wrap
            } else {
                nextQueueFrontIndex++;
            }
            this.queueTempFrontIndex.set(nextQueueFrontIndex);
            
            return data;
        } finally {
        	queueFrontWriteLock.unlock();
        }

    }
    
    @Override
    public void uncommit() {
    	try {
            queueFrontWriteLock.lock();
            queueTempFrontIndex.set(queueFrontIndex.get());
    	} finally {
            queueFrontWriteLock.unlock();
    	}
    }
    
    public void commit() throws IOException {
    	try {
            queueFrontWriteLock.lock();
            if (queueTempFrontIndex.get() < queueFrontIndex.get()) {
            	throw new RuntimeException(String.format("Can not commit index less than the recorded committed index: %d < %d", queueTempFrontIndex.get(), queueFrontIndex.get()));
            }
            queueFrontIndex.set(queueTempFrontIndex.get());
            
            // persist the queue front
            IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(queueFrontIndex.get());
            queueFrontIndexPage.setDirty(true);
    	} finally {
            queueFrontWriteLock.unlock();
    	}
    }

    @Override
    public ListenableFuture<byte[]> dequeueAsync() {
        this.initializeDequeueFutureIfNecessary();
        return dequeueFuture;
    }

    @Override
    public void removeAll() throws IOException {
        try {
            queueFrontWriteLock.lock();
            this.innerArray.removeAll();
            this.queueFrontIndex.set(0L);
            this.queueTempFrontIndex.set(0L);
            IMappedPage queueFrontIndexPage = this.queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocal(0);
            queueFrontIndexBuffer.putLong(0L);
            queueFrontIndexPage.setDirty(true);
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public byte[] peek() throws IOException {
        if (this.isEmpty()) {
            return null;
        }
        byte[] data = this.innerArray.get(this.queueFrontIndex.get());
        return data;
    }
    
    @Override
    public byte[] peekLast() throws IOException {
        if (this.isEmpty()) {
            return null;
        }
        
        try {
            queueFrontWriteLock.lock();
            byte[] data = this.innerArray.get(this.innerArray.getHeadIndex() - 1);
            return data;
        } finally {
        	queueFrontWriteLock.unlock();
        }
    }

    @Override
    public ListenableFuture<byte[]> peekAsync() {
        this.initializePeekFutureIfNecessary();
        return peekFuture;
    }

    /**
     * apply an implementation of a ItemIterator interface for each queue item
     *
     * @param iterator
     * @throws IOException
     */
    @Override
    public void applyForEach(ItemIterator iterator) throws IOException {
        try {
            queueFrontWriteLock.lock();
            if (this.isEmpty()) {
                return;
            }

            long index = this.queueFrontIndex.get();
            for (long i = index; i < this.innerArray.size(); i++) {
                iterator.forEach(this.innerArray.get(i));
            }
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        if (this.queueFrontIndexPageFactory != null) {
            this.queueFrontIndexPageFactory.releaseCachedPages();
        }


        synchronized (futureLock) {
            /* Cancel the future but don't interrupt running tasks
            because they might perform further work not refering to the queue
             */
            if (peekFuture != null) {
                peekFuture.cancel(false);
            }
            if (dequeueFuture != null) {
                dequeueFuture.cancel(false);
            }
        }

        this.innerArray.close();
        this.pidPageFactory.releaseLock(PID_DATA_PAGE_INDEX);
    }

    @Override
    public void gc() throws IOException {
        long beforeIndex = this.queueFrontIndex.get();
        if (beforeIndex == 0L) { // wrap
            beforeIndex = Long.MAX_VALUE;
        } else {
            beforeIndex--;
        }
        try {
            this.innerArray.removeBeforeIndex(beforeIndex);
        } catch (IndexOutOfBoundsException ex) {
            // ignore
        }
    }

    @Override
    public void flush() {
        try {
            queueFrontWriteLock.lock();
            this.queueFrontIndexPageFactory.flush();
            this.innerArray.flush();
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public long size() {
        long qFront = this.queueFrontIndex.get();
        long qRear = this.innerArray.getHeadIndex();
        if (qFront <= qRear) {
            return (qRear - qFront);
        } else {
            return Long.MAX_VALUE - qFront + 1 + qRear;
        }
    }


    /**
     * Completes the dequeue future
     */
    private void completeFutures() {
        synchronized (futureLock) {
            if (peekFuture != null && !peekFuture.isDone()) {
                try {
                    peekFuture.set(this.peek());
                } catch (IOException e) {
                    peekFuture.setException(e);
                }
            }
            if (dequeueFuture != null && !dequeueFuture.isDone()) {
                try {
                    dequeueFuture.set(this.dequeue());
                } catch (IOException e) {
                    dequeueFuture.setException(e);
                }
            }
        }
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializeDequeueFutureIfNecessary() {
        synchronized (futureLock) {
            if (dequeueFuture == null || dequeueFuture.isDone()) {
                dequeueFuture = SettableFuture.create();
            }
            if (!this.isEmpty()) {
                try {
                    dequeueFuture.set(this.dequeue());
                } catch (IOException e) {
                    dequeueFuture.setException(e);
                }
            }
        }
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializePeekFutureIfNecessary() {
        synchronized (futureLock) {
            if (peekFuture == null || peekFuture.isDone()) {
                peekFuture = SettableFuture.create();
            }
            if (!this.isEmpty()) {
                try {
                    peekFuture.set(this.peek());
                } catch (IOException e) {
                    peekFuture.setException(e);
                }
            }
        }
    }

}
