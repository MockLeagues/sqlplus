package com.tyler.sqlplus.functional;

@FunctionalInterface
public interface Work<T> {

	public void doWork(T in) throws Exception;
	
}
