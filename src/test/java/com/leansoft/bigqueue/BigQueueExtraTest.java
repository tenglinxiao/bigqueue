package com.leansoft.bigqueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BigQueueExtraTest {
	private IBigQueue queue;
	
	@Before
	public void setup() throws IOException {
		queue = new BigQueueImpl("/opt/data/hermes/filequeue", "test", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE, 1);
		queue.removeAll();
	}
	
	@Test
	public void testDequeue() throws IOException {
		for (int index = 0; index < 10; index++) {
			String data = String.valueOf(index);
			queue.enqueue(data.getBytes());
		}
		
		byte[] data1 = queue.dequeue();
		byte[] data2 = queue.dequeue();
		
		Assert.assertEquals(String.valueOf(0), new String(data1));
		Assert.assertEquals(String.valueOf(1), new String(data2));
	}
	
	@Test
	public void testUnCommit() throws IOException {
		for (int index = 0; index < 10; index++) {
			String data = String.valueOf(index);
			queue.enqueue(data.getBytes());
		}
		
		byte[] data1 = queue.dequeue();
		
		queue.uncommit();
		
		byte[] data2 = queue.dequeue();
		
		Assert.assertTrue(Arrays.equals(data1, data2));
	}
	
	@Test
	public void testCommit() throws IOException {
		for (int index = 0; index < 10; index++) {
			String data = String.valueOf(index);
			queue.enqueue(data.getBytes());
		}
		
		queue.dequeue();
		queue.dequeue();
		
		queue.commit();
		Assert.assertEquals(new String(queue.dequeue()), String.valueOf(2));
		
		Assert.assertEquals(queue.size(), 8);
		queue.commit();
		Assert.assertEquals(queue.size(), 7);
	}
	
	@Test(expected = ExecutionException.class)
	public void testPid() throws Throwable {
		for (int index = 0; index < 10; index++) {
			String data = String.valueOf(index);
			queue.enqueue(data.getBytes());
		}
		
		Assert.assertEquals(queue.size(), 10);
		
		Future<?> future = Executors.newSingleThreadExecutor().submit(new Runnable() {

			@Override
			public void run() {
				try {
					new BigQueueImpl("/opt/data/hermes/filequeue", "test");
				} catch (IOException ex) {
				}
			}
			
		});
		
		try {
			future.get();
		} catch (ExecutionException e) {
			throw e;
		}
		
	}
	
	@Test(expected = IOException.class)
	public void testMaxAllowedPages() throws IOException {
		int index = 0;
		byte[] bytes = new byte[1024];
		Random random = new Random();
		try {
			while (index++ < Integer.MAX_VALUE) {
				random.nextBytes(bytes);
				queue.enqueue(bytes);
			}
		} finally {
			Assert.assertEquals(queue.size(), 32 * 1024);
		}
		
	}
	
	@After
	public void tearDown() throws IOException {
		queue.close();
	}
	
}
