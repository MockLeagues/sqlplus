package com.tyler.sqlplus;

import java.sql.Connection;

/**
 * Defines the contract for a section of code which consumes a database connection and returns a value
 */
@FunctionalInterface
public interface ReturningDBWork<T> {

	public T query(Connection conn) throws Exception;
	
}
