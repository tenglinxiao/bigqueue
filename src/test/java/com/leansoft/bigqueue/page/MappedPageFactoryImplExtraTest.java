package com.leansoft.bigqueue.page;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

public class MappedPageFactoryImplExtraTest {

	@Test(expected = IOException.class)
	public void testMaxAllowedPages() throws IOException {
		int maxAllowedPages = 3;
		MappedPageFactoryImpl pageFactory = new MappedPageFactoryImpl(32, "/opt/data/hermes/filequeue/unittest/data", 1000, maxAllowedPages);
		pageFactory.deleteAllPages();
		
		int index = 0;
		try {
			while(index < 4) {
				pageFactory.acquirePage(index++);
			}
		} finally {
			Assert.assertEquals(index, 4);
		}
	}
	
	@Test(expected = IOException.class)
	public void testMaxAllowedPagesWithinRemoval() throws IOException {
		int maxAllowedPages = 3;
		MappedPageFactoryImpl pageFactory = new MappedPageFactoryImpl(32, "/opt/data/hermes/filequeue/unittest/data", 1000, maxAllowedPages);
		pageFactory.deletePagesBeforePageIndex(2);
		int index = 3;
		try {
			while(index < 6) {
				pageFactory.acquirePage(index++);
			}
		} finally {
			Assert.assertEquals(index, 6);
		}
	}
}
