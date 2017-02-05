package com.tyler.sqlplus.function;

import com.tyler.sqlplus.exception.SQLRuntimeException;

import java.sql.SQLException;

public interface Functions {

	@FunctionalInterface
	interface ThrowingRunnable {

		void run() throws Exception;

	}

	@FunctionalInterface
	interface ThrowingSupplier<O> {

		O get() throws Exception;

	}

	@FunctionalInterface
	interface SQLExceptionSupplier<O> {

		O run() throws SQLException;

	}

	@FunctionalInterface
	interface SQLExceptionRunnable {

		void run() throws SQLException;

	}
	
	@FunctionalInterface
	interface ThrowingFunction<I, O> {
		
		O apply(I in) throws Exception;
		
	}

	@FunctionalInterface
	interface ThrowingConsumer<I> {

		void accept(I in) throws Exception;

	}

	@FunctionalInterface
	interface ThrowingBiConsumer<I, E> {

		void accept(I in1, E in2) throws Exception;

	}

	static <O> O get(ThrowingSupplier<O> supplier) {
		try {
			return supplier.get();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	static void run(ThrowingRunnable action) {
		try {
			action.run();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	static <O> O runSQL(SQLExceptionSupplier<O> supplier) {
		try {
			return supplier.run();
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	static void runSQL(SQLExceptionRunnable runnable) {
		try {
			runnable.run();
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
}
