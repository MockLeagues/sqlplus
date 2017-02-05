package com.tyler.sqlplus.function;

import com.tyler.sqlplus.exception.SQLRuntimeException;

import java.sql.SQLException;

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
	public static interface SQLExceptionRunnable {
		
		void run() throws SQLException;
		
	}
	
	@FunctionalInterface
	public static interface ThrowingFunction<I, O> {
		
		O apply(I in) throws Exception;
		
	}

	@FunctionalInterface
	static interface ThrowingConsumer<I> {

		void accept(I in) throws Exception;

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
	
	public static <O> O runSQL(SQLExceptionSupplier<O> supplier) {
		try {
			return supplier.run();
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	public static void runSQL(SQLExceptionRunnable runnable) {
		try {
			runnable.run();
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
}
