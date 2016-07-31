package com.tyler.sqlplus;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Middle-man class to provide a clean interface for producing query objects from a raw JDBC connection
 */
public class SqlPlusConnection implements Closeable {

	private Connection conn;
	
	public SqlPlusConnection(Connection conn) {
		this.conn = conn;
	}
	
	public Query createQuery(String sql) {
		return new Query(sql, conn);
	}

	@Override
	public void close() throws IOException {
		try {
			conn.close();
		}
		catch (SQLException e) {
			throw new IOException(e);
		}
	}
	
}
