package com.tyler.sqlplus.functional;

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
	
}
