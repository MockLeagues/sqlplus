package com.tyler.sqlplus.function;

@FunctionalInterface
public interface ReturningWork<I, O> {

	public O doReturningWork(I in) throws Exception;
	
}
