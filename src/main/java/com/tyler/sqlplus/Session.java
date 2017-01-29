package com.tyler.sqlplus;

import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.exception.SessionClosedException;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an individual unit of work within the SqlPlus environment
 */
public class Session implements Closeable {

	private Map<QueryCacheKey, Object> firstLevelCache = new HashMap<>();
	private boolean lastResultCached = false;

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

	public boolean wasFromCache() {
		return lastResultCached;
	}

	/**
	 * Flushes current transaction data to the database
	 */
	public void flush() {
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	/**
	 * Stages a new query within this session. If the session is no longer active, a {@link SessionClosedException}
	 * will be thrown
	 */
	public Query createQuery(String sql) {
		assertOpen();
		return new Query(sql, this);
	}

	void invalidateFirstLevelCache() {
		firstLevelCache.clear();
	}

	<T> T getUniqueResult(Query query, Class<T> resultClass) {
		lastResultCached = true;
		return (T) firstLevelCache.computeIfAbsent(new QueryCacheKey(query, resultClass), q -> {
			lastResultCached = false;
			return query.getUniqueResultForCache(resultClass);
		});
	}

	<T> List<T> fetch(Query query, Class<T> resultClass) {
		lastResultCached = true;
		return (List<T>) firstLevelCache.computeIfAbsent(new QueryCacheKey(query, resultClass), q -> {
			lastResultCached = false;
			return query.fetchForCache(resultClass);
		});
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

/**
 * Allows queries to be cached according to not just the query but also its intended result type.
 * This allows the same query to be interpreted as different result types in the same session. Otherwise,
 * query results would be cached as the first result type it was interpreted as
 */
class QueryCacheKey {

	private Query query;
	private Class<?> resultClass;

	public QueryCacheKey(Query query, Class<?> resultClass) {
		this.query = query;
		this.resultClass = resultClass;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof QueryCacheKey) {
			QueryCacheKey other = (QueryCacheKey) o;
			return Objects.equals(query, other.query) && Objects.equals(resultClass, other.resultClass);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(query, resultClass);
	}

}