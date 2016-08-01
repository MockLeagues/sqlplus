package com.tyler.sqlplus.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

public class EntityProxyFactoryTest {

	@Test
	public void testDetermineCollectionImpl() throws Exception {
		
		assertEquals(new ArrayList<>(), EntityProxyFactory.chooseCollectionImpl(Collection.class));
		
		assertEquals(new ArrayList<>(), EntityProxyFactory.chooseCollectionImpl(List.class));
		assertEquals(new ArrayList<>(), EntityProxyFactory.chooseCollectionImpl(ArrayList.class));
		assertEquals(new LinkedList<>(), EntityProxyFactory.chooseCollectionImpl(LinkedList.class));
		
		assertEquals(new HashSet<>(), EntityProxyFactory.chooseCollectionImpl(Set.class));
		assertEquals(new TreeSet<>(), EntityProxyFactory.chooseCollectionImpl(SortedSet.class));
		assertEquals(new HashSet<>(), EntityProxyFactory.chooseCollectionImpl(HashSet.class));
		assertEquals(new LinkedHashSet<>(), EntityProxyFactory.chooseCollectionImpl(LinkedHashSet.class));
		assertEquals(new TreeSet<>(), EntityProxyFactory.chooseCollectionImpl(TreeSet.class));
		
		assertEquals(new LinkedList<>(), EntityProxyFactory.chooseCollectionImpl(Deque.class));
		assertEquals(new LinkedList<>(), EntityProxyFactory.chooseCollectionImpl(Queue.class));
		
		assertTrue(EntityProxyFactory.chooseCollectionImpl(PriorityQueue.class) instanceof PriorityQueue);
	}
	
}
