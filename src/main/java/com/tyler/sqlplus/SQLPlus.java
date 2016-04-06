package com.tyler.sqlplus;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
	public <T> List<T> fetch(Class<T> pojoClass, String sql) {
		return query(conn -> conn.createQuery(sql).fetchAs(pojoClass));
	}
	
	/**
	 * Shortcut method for creating a query which immediately finds a single instance of a mapped POJO
	 */
	public <T> T find(Class<T> pojoClass, String sql) {
		return query(conn -> conn.createQuery(sql).findAs(pojoClass));
	}
	
	/**
	 * Shortcut method which immediately executes a callback for each mapped result of the given query.
	 * 
	 * This method is ideal for processing large result sets, since each mapped instance will be garbage
	 * collected after each invocation of the callback (therefore preserving memory consumption)
	 */
	public <T> void forEach(Class<T> pojoClass, String sql, Consumer<T> action) {
		transact(conn -> conn.createQuery(sql, pojoClass).stream().forEach(action));
	}
	
	/**
	 * Shortcut method for a query which filters the global result set for the given POJO class.
	 * 
	 * Extreme caution should be taken when using this method; it will cause a full result-set query
	 * of the given POJO type (determined by the query in the POJO's @GlobalQuery class-level annotation).
	 * For this reason, this method should only be used for relatively small result sets
	 */
	public <T> List<T> filter(Class<T> pojo, Predicate<T> filter) {
		return query(conn -> conn.globalStream(pojo).filter(filter).collect(Collectors.toList()));
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
