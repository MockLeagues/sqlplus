package com.tyler.sqlplus;

import com.tyler.sqlplus.rule.H2Rule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertTrue;

public class SQLPlusTest {

	@Rule
	public H2Rule h2 = new H2Rule();
	
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
