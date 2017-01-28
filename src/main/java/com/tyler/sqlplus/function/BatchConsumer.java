package com.tyler.sqlplus.function;

import java.sql.SQLException;
import java.util.List;

@FunctionalInterface
public interface BatchConsumer<T> {

	public void acceptBatch(List<T> batch) throws SQLException;
	
}
