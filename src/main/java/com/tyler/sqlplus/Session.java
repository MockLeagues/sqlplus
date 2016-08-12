package com.tyler.sqlplus;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import com.tyler.sqlplus.exception.SessionClosedException;
import com.tyler.sqlplus.exception.SqlRuntimeException;

/**
 * Represents an individual unit of work within the SqlPlus environment
 */
public class Session implements Closeable {

	private SqlPlus context;
	private Connection conn;
	
	public Session(Connection conn, SqlPlus context) {
		this.conn = conn;
		this.context = context;
	}
	
	/**
	 * Stages a new query within this session. If the session is no longer active, a {@link SessionClosedException}
	 * will be thrown
	 */
	public Query createQuery(String sql) {
		try {
			if (conn.isClosed()) {
				throw new SessionClosedException();
			}
		} catch (SQLException e) {
			throw new SqlRuntimeException(e);
		}
		return new Query(sql, this);
	}

	public boolean isOpen() {
		try {
			return !conn.isClosed();
		}
		catch (SQLException e) {
			return false;
		}
	}
	
	Connection getJdbcConnection() {
		return conn;
	}
	
	public SqlPlus getContext() {
		return context;
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
