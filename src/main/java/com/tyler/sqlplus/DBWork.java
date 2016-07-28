package com.tyler.sqlplus;

import java.sql.Connection;

/**
 * Defines the contract for a section of code which consumes a database connection
 */
@FunctionalInterface
public interface DBWork {

	public void transact(Connection conn) throws Exception;
	
}
