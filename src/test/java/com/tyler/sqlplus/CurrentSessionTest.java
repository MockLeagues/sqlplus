package com.tyler.sqlplus;

import com.tyler.sqlplus.base.DatabaseTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class SQLPlusTest extends DatabaseTest {

	@Test
	public void sessionShouldBeSharedThroughoutCurrentThreadStack() throws Exception {
		
		List<Session> sessionsRetrieved = new ArrayList<>();
		
		Callable<Object> childCall = () -> {
			db.getSQLPlus().transact(conn -> {
				sessionsRetrieved.add(conn);
			});
			return null;
		};
		
		Callable<Object> parentCall = () -> {
			db.getSQLPlus().transact(sess -> {
				sessionsRetrieved.add(sess);
				childCall.call(); // When the child call opens it's session, the session it gets should be the exact same object
			});
			return null;
		};
		
		parentCall.call();
		assertTrue(sessionsRetrieved.get(0) == sessionsRetrieved.get(1));
	}

	@Test
	public void newSessionOpenedInThreadShouldBeDifferentFromActiveSessionInAnotherThread() throws Exception {

		Session[] threadSessions = { null, null };

		Thread threadA = new Thread(() -> {
			db.getSQLPlus().transact(sess -> {
				threadSessions[0] = sess;
				Thread.sleep(100); // Simulate active session
			});
		});

		Thread threadB = new Thread(() -> {
			db.getSQLPlus().transact(sess -> {
				threadSessions[1] = sess;
			});
		});

		threadA.start();
		threadB.start();
		threadA.join();
		threadB.join();

		assertFalse(threadSessions[0] == threadSessions[1]);
	}

}
