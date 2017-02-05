package com.tyler.sqlplus.keyprovider;

import com.tyler.sqlplus.Session;

import java.sql.SQLException;

/**
 * Provides primary key values for new database entities
 */
@FunctionalInterface
public interface KeyProvider<T> {

	/**
	 * This class exists purely to provide a default value for the update annotation
	 */
	final class VoidKeyProvider implements KeyProvider<Void> {

		@Override
		public Void getKey(Session session) throws SQLException {
			return null;
		}

	}

	T getKey(Session session) throws SQLException;

}
