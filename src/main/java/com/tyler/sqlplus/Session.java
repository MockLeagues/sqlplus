package com.tyler.sqlplus;

import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.exception.SessionClosedException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Represents an individual unit of work within the SqlPlus environment
 */
public class Session implements Closeable {

	/**
	 * Package-private so as to not break encapsulation.
	 * JDBC connection object should ONLY every be retrieved from the Query class
	 */
	Connection conn;
	
	Session(Connection conn) {
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

	/**
	 * Flushes current transaction data to the database
	 */
	public void flush() {
		try {
			conn.commit();
		}
		catch (SQLException e) {
			throw new SQLRuntimeException(e);
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

	void rollback() {
		try {
			conn.rollback();
			conn.close();
		}
		catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}

	private void assertOpen() {
		if (!isOpen()) {
			throw new SessionClosedException();
		}
	}

	public void closeQuiet() {
		try {
			close();
		} catch(IOException e) {
			throw new RuntimeException(e);
		}
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