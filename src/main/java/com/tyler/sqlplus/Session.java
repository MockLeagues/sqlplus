package com.tyler.sqlplus;

import com.tyler.sqlplus.exception.SessionClosedException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Represents an individual unit of work within the SqlPlus environment
 */
public class Session implements Closeable {

	private Connection conn;
	
	public Session(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Convenience method to create a query with parameters in a single call
	 */
	public Query createQuery(String sql, Object... params) {
		Query q = createQuery(sql);
		for (int i = 0; i < params.length; i++) {
			q.setParameter(i + 1, params[i]);
		}
		return q;
	}

	/**
	 * Stages a new query within this session. If the session is no longer active, a {@link SessionClosedException}
	 * will be thrown
	 */
	public Query createQuery(String sql) {
		assertOpen();
		return new Query(sql, this);
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
