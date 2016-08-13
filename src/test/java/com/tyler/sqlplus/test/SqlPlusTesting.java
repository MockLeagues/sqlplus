package com.tyler.sqlplus.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.tyler.sqlplus.functional.Task;

public class SqlPlusTesting {

	public static void assertThrows(Task t, Class<? extends Throwable> expectType) {
		assertThrows(t, expectType, null);
	}
	
	public static void assertThrows(Task t, Class<? extends Throwable> expectType, String expectMsg) {
		try {
			t.run();
			fail("Expected test to throw instance of " + expectType.getName() + " but no error was thrown");
		}
		catch (Throwable thrownError) {
			if (thrownError.getClass() == AssertionError.class) {
				throw new RuntimeException(thrownError);
			}
			if (!expectType.equals(thrownError.getClass())) {
				fail("Expected test to throw instance of " + expectType.getName() + " but no instead got error of type " + thrownError.getClass().getName());
			}
			if (expectMsg != null) {
				assertEquals(expectMsg, thrownError.getMessage());
			}
		}
	}
	
}
