package com.tyler.sqlplus;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Represents an individual unit of work within the SqlPlus environment
 */
public class SqlPlusSession implements Closeable {

	private Connection conn;
	
	public SqlPlusSession(Connection conn) {
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
