package com.tyler.sqlplus;

import java.sql.SQLException;

/**
 * Provides primary key values for new database entities
 */
public interface KeyProvider<T> {

	final class VoidKeyProvider implements KeyProvider<Void> {

		@Override
		public Void getKey(Session session) throws SQLException {
			return null;
		}

	}

	T getKey(Session session) throws SQLException;

}
