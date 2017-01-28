package com.tyler.sqlplus;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Rule;
import org.junit.Test;

import com.tyler.sqlplus.rule.H2EmployeeDBRule;

public class SQLPlusTest {

	@Rule
	public H2EmployeeDBRule h2 = new H2EmployeeDBRule();
	
	@Test
	public void testCurrentThreadCorrectlyPullsCurrentSession() throws Exception {
		
		List<Session> sessionsRetrieved = new ArrayList<>();
		
		Callable<Object> childCall = () -> {
			h2.getSQLPlus().transact(conn -> {
				sessionsRetrieved.add(conn);
			});
			return null;
		};
		
		Callable<Object> parentCall = () -> {
			h2.getSQLPlus().transact(sess -> {
				sessionsRetrieved.add(sess);
				childCall.call(); // When the child call opens it's session, the session it gets should be the exact same object
			});
			return null;
		};
		
		parentCall.call();
		assertTrue(sessionsRetrieved.get(0) == sessionsRetrieved.get(1));
	}
	
}
