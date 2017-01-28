package com.tyler.sqlplus.function;

import java.sql.SQLException;

import com.tyler.sqlplus.exception.SqlRuntimeException;

public interface Functions {

	@FunctionalInterface
	public static interface ThrowingSupplier<O> {
		
		public O get() throws Exception;
		
	}

	@FunctionalInterface
	public static interface ThrowingRunnable {
		
		public void run() throws Exception;
		
	}
	
	@FunctionalInterface
	public static interface SQLExceptionSupplier<O> {
		
		public O run() throws SQLException;
		
	}
	
	@FunctionalInterface
	public static interface ThrowingFunction<I, O> {
		
		public O apply(I in) throws Exception;
		
	}
	
	public static <O> O get(ThrowingSupplier<O> supplier) {
		try {
			return supplier.get();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void run(ThrowingRunnable action) {
		try {
			action.run();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static <O> O runSQL(SQLExceptionSupplier<O> runnable) {
		try {
			return runnable.run();
		} catch (SQLException e) {
			throw new SqlRuntimeException(e);
		}
	}
	
}