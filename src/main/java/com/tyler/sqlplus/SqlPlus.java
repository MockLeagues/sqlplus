package com.tyler.sqlplus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.tyler.sqlplus.exception.ConfigurationException;
import com.tyler.sqlplus.exception.SqlRuntimeException;
import com.tyler.sqlplus.functional.ReturningWork;
import com.tyler.sqlplus.functional.Work;
import com.tyler.sqlplus.proxy.TransactionAwareService;

/**
 * This class is the primary entry point to the SQLPlus API.
 * 
 * An instance of a SQLPLus object provides an interface for executing actions against a database connection
 */
public class SqlPlus {

	private Configuration config;
	private Supplier<Connection> connectionFactory;
	private Map<Long, Session> id_currentSession = new HashMap<>();
	
	public SqlPlus(String url, String user, String pass) {
		this(new Configuration().setUrl(url).setUsername(user).setPassword(pass));
	}

	public SqlPlus(Configuration config) {
		this(config, () -> {
			try {
				return DriverManager.getConnection(config.getUrl(), config.getUsername(), config.getPassword());
			}
			catch (Exception ex) {
				throw new ConfigurationException("Failed to connect to database", ex);
			}
		});
	}

	public SqlPlus(Supplier<Connection> factory) {
		this(new Configuration(), factory);
	}
	
	public SqlPlus(Configuration config, Supplier<Connection> factory) {
		
		String driverClass = config.getDriverClass();
		if (driverClass != null) {
			try {
				Class.forName(driverClass);
			} catch (ClassNotFoundException e) {
				throw new ConfigurationException("Driver class " + driverClass + " not found", e);
			}
		}
		
		this.config = config;
		this.connectionFactory = factory;
	}

	public <T> T createTransactionAwareService(Class<T> klass) throws InstantiationException, IllegalAccessException {
		return TransactionAwareService.create(klass, this);
	}
	
	public void testConnection() {
		open(conn -> {
			// Throws if problems opening connection
		});
	}

	/**
	 * Executes an action against a database connection obtained from this instance's connection factory
	 */
	public void open(Work<Session> action) {
		exec(conn -> {
			action.doWork(conn);
			return null;
		}, false);
	}
	
	/**
	 * Executes an action inside of a single database transaction.
	 * 
	 * If any exceptions are thrown, the transaction is immediately rolled back
	 */
	public void transact(Work<Session> action) {
		exec(conn -> {
			action.doWork(conn);
			return null;
		}, true);
	}

	/**
	 * Executes a value-returning action against a database connection obtained from this instance's connection factory
	 */
	public <T> T query(ReturningWork<Session, T> action) {
		return exec(action, false);
	}

	/**
	 * Private method through which all other sql plus session method interfaces filter into
	 */
	private <T> T exec(ReturningWork<Session, T> action, boolean transactional) {

		Long currentSessionId = Thread.currentThread().getId();
		if (id_currentSession.containsKey(currentSessionId)) {
			Session currentSession = id_currentSession.get(currentSessionId);
			try {
				return action.doReturningWork(currentSession);
			} catch (Exception e) {
				throw new SqlRuntimeException(e);
			}
		}
		
		Connection conn = null;
		T result = null;

		try {
			conn = connectionFactory.get();
			if (transactional) {
				conn.setAutoCommit(false);
			}
			Session newSession = new Session(conn, config);
			id_currentSession.put(currentSessionId, newSession);
			result = action.doReturningWork(newSession);
			if (transactional) {
				conn.commit();
			}
		}
		catch (Exception e) {
			id_currentSession.remove(currentSessionId);
			if (conn != null) {
				try {
					if (transactional) {
						conn.rollback();
					}
					conn.close();
				} catch (SQLException e2) {
					throw new SqlRuntimeException(e2);
				}
			}
			throw new SqlRuntimeException(e);
		}

		id_currentSession.remove(currentSessionId);
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				throw new SqlRuntimeException(e);
			}
		}
		
		return result;
	}
	
	public int[] batchExec(String... stmts) {
		try (Connection conn = connectionFactory.get()) {
			Statement s = conn.createStatement();
			for (String sql : stmts) {
				s.addBatch(sql);
			}
			return s.executeBatch();
		}
		catch (SQLException e) {
			throw new SqlRuntimeException(e);
		}
	}

}
