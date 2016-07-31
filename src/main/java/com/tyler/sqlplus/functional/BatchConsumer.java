package com.tyler.sqlplus.functional;

import java.util.List;

@FunctionalInterface
public interface BatchConsumer<T> {

	public void acceptBatch(List<T> batch) throws Exception;
	
}
