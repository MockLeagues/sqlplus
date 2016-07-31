package com.tyler.sqlplus.functional;

@FunctionalInterface
public interface ReturningWork<I, O> {

	public O doReturningWork(I in) throws Exception;
	
}
