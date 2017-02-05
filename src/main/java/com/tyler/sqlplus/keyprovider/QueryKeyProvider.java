package com.tyler.sqlplus.keyprovider;

import com.tyler.sqlplus.Session;

import java.sql.SQLException;

/**
 * Key provider which provides keys from a simple SQL query
 */
public class QueryKeyProvider<T> implements KeyProvider<T> {

	private String keyQuerySQL;
	private Class<T> keyClass;

	public QueryKeyProvider(String keyQuerySQL, Class<T> keyClass) {
		this.keyQuerySQL = keyQuerySQL;
		this.keyClass = keyClass;
	}

	@Override
	public T getKey(Session session) throws SQLException {
		return session.createQuery(keyQuerySQL).getUniqueResultAs(keyClass);
	}

}
