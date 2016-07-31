package com.tyler.sqlplus;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Rule;
import org.junit.Test;

import com.tyler.sqlplus.rule.H2EmployeeDBRule;

public class SqlPlusTest {

	@Rule
	public H2EmployeeDBRule h2 = new H2EmployeeDBRule();
	
	@Test
	public void testCurrentThreadSessionIDModeCorrectlyPullsCurrentSession() throws Exception {
		
		h2.getSQLPlus().setSessionIDMode(SessionIDMode.CURRENT_THREAD);
		
		List<SqlPlusSession> sessionsRetrieved = new ArrayList<>();
		
		Callable<Object> childCall = () -> {
			h2.getSQLPlus().open(conn -> {
				sessionsRetrieved.add(conn);
			});
			return null;
		};
		
		Callable<Object> parentCall = () -> {
			h2.getSQLPlus().open(sess -> {
				sessionsRetrieved.add(sess);
				childCall.call(); // When the child call opens it's session, the session it gets should be the exact same object
			});
			return null;
		};
		
		parentCall.call();
		assertTrue(sessionsRetrieved.get(0) == sessionsRetrieved.get(1));
	}
	
//	@Test
//	public void testDataSourceSessionIDModeCorrectlyPullsCurrentSession() throws Exception {
//		
//		h2.getSQLPlus().setSessionIDMode(SessionIDMode.DATA_SOURCE);
//		
//		List<SqlPlusSession> sessionsRetrieved = new ArrayList<>();
//		
//		CountDownLatch latch = new CountDownLatch(2);
//		
//		Thread t1 = new Thread(() -> {
//			h2.getSQLPlus().open(sessionsRetrieved::add);
//			latch.countDown();
//		});
//		
//		Thread t2 = new Thread(() -> {
//			h2.getSQLPlus().open(sessionsRetrieved::add);
//			latch.countDown();
//		});
//		
//		t1.start();
//		t2.start();
//		latch.await();
//		System.out.println(sessionsRetrieved);
//		assertTrue(sessionsRetrieved.get(0) == sessionsRetrieved.get(1));
//	}
	
}
