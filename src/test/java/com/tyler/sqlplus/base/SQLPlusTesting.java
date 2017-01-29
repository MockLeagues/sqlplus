package com.tyler.sqlplus.base;

import com.tyler.sqlplus.function.Task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SQLPlusTesting {

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
