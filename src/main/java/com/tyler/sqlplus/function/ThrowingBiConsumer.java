package com.tyler.sqlplus.function;

@FunctionalInterface
public interface ThrowingBiConsumer<I, E> {

	public void accept(I in1, E in2) throws Exception;
	
}
