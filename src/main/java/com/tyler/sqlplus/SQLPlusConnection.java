package com.tyler.sqlplus;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.tyler.sqlplus.query.Query;
import com.tyler.sqlplus.query.TypedQuery;

/**
 * Wrapper over a standard JDBC connection
 */
public class SQLPlusConnection implements Closeable {

	private Connection conn;
	
	public SQLPlusConnection(Connection conn) {
		this.conn = conn;
	}

	public Query createQuery(String sql) {
		return new Query(sql, conn);
	}
	
	public <T> TypedQuery<T> createQuery(String sql, Class<T> type) {
		return new TypedQuery<>(type, sql, conn);
	}
	
	@Override
	public void close() throws IOException {
		try {
			this.conn.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
}
