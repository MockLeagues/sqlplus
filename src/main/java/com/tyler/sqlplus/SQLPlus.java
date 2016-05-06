package com.tyler.sqlplus;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.tyler.sqlplus.exception.ConfigurationException;

public class SQLPlus {

	private Supplier<Connection> connectionFactory;
	
	public SQLPlus(Supplier<Connection> factory) {
		this.connectionFactory = factory;
	}
	
	public SQLPlus(String url, String user, String pass) {
		this(() -> {
			try {
				return DriverManager.getConnection(url, user, pass);
			}
			catch (Exception ex) {
				throw new ConfigurationException("Failed to connect to database", ex);
			}
		});
	}
	
	public void testConnection() {
		connectionFactory.get(); // Throws if problems
	}
	
	/**
	 * Executes an action against a new database connection
	 */
	public void transact(Consumer<SQLPlusConnection> action) {
		try (SQLPlusConnection conn = new SQLPlusConnection(connectionFactory.get())) {
			action.accept(conn);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Executes a value-returning action against a new database connection
	 */
	public <T> T query(Function<SQLPlusConnection, T> action) {
		try (SQLPlusConnection conn = new SQLPlusConnection(connectionFactory.get())) {
			return action.apply(conn);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Shortcut method for creating a query which immediately returns a list of mapped POJOs
	 */
	public <T> List<T> fetch(Class<T> pojoClass, String sql, Object... params) {
		return query(conn -> conn.createDynamicQuery().query(sql, params).build().fetchAs(pojoClass));
	}
	
	/**
	 * Shortcut method for creating a query which immediately returns simple maps for each result row
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> fetchMaps(String sql, Object... params) {
		return (Map<String, Object>) query(conn -> {
			return conn.createDynamicQuery().query(sql, params).build().fetchMaps();
		});
	}
	
	/**
	 * Shortcut method for creating a query which immediately finds a single instance of a mapped POJO
	 */
	public <T> T find(Class<T> pojoClass, String sql, Object... params) {
		return query(conn -> conn.createDynamicQuery().query(sql, params).build().findAs(pojoClass));
	}
	
	/**
	 * Shortcut method for querying a single integer scalar value
	 */
	public int queryInt(String sql) {
		return queryScalar(Integer.class, sql);
	}
	
	/**
	 * Shortcut method for querying a single double scalar value
	 */
	public double queryDouble(String sql) {
		return queryScalar(Double.class, sql);
	}
	
	/**
	 * Shortcut method for querying a single boolean scalar value
	 */
	public boolean queryBoolean(String sql) {
		return queryScalar(Boolean.class, sql);
	}
	
	/**
	 * Shortcut method for querying a single string scalar value
	 */
	public String queryString(String sql) {
		return queryScalar(String.class, sql);
	}
	
	/**
	 * Shortcut method for querying a single character scalar value
	 */
	public char queryChar(String sql) {
		return queryScalar(Character.class, sql);
	}
	
	/**
	 * Shortcut method for pulling a scalar value from a query
	 */
	private <T> T queryScalar(Class<T> scalarClass, String sql) {
		return query(conn -> conn.createQuery(sql).fetchScalar(scalarClass));
	}
	
}
