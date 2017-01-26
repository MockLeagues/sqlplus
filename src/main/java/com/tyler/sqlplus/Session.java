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

	private Connection conn;
	
	public Session(Connection conn) {
		this.conn = conn;
	}
	
	/**
	 * Stages a new query within this session. If the session is no longer active, a {@link SessionClosedException}
	 * will be thrown
	 */
	public Query createQuery(String sql) {
		assertOpen();
		return new Query(sql, this);
	}
	
	public void setTransactionIsolationLevel(int level) {
		assertOpen();
		try {
			conn.setTransactionIsolation(level);
		} catch (SQLException e) {
			throw new SqlRuntimeException(e);
		}
	}
	
	private void assertOpen() {
		if (!isOpen()) {
			throw new SessionClosedException();
		}
	}
	
	public boolean isOpen() {
		try {
			return !conn.isClosed();
		}
		catch (SQLException e) {
			return false;
		}
	}
	
	/**
	 * Package-private so as to not break encapsulation.
	 * JDBC connection object should ONLY every be retrieved from the Query class
	 */
	Connection getJdbcConnection() {
		return conn;
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
