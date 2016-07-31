package com.tyler.sqlplus.functional;

import java.sql.Connection;

/**
 * Defines the contract for a section of code which consumes a database connection
 */
@FunctionalInterface
public interface DBWork {

	public void doWork(Connection conn) throws Exception;
	
}
