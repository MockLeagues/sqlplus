package com.tyler.sqlplus.function;

@FunctionalInterface
public interface Work<T> {

	public void doWork(T in) throws Exception;
	
}
