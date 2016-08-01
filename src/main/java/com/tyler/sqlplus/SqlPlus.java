package com.tyler.sqlplus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import javax.sql.DataSource;

import com.tyler.sqlplus.exception.ConfigurationException;
import com.tyler.sqlplus.exception.SqlRuntimeException;
import com.tyler.sqlplus.functional.ReturningWork;
import com.tyler.sqlplus.functional.Work;

/**
 * This class is the primary entry point to the SQLPlus API.
 * 
 * An instance of a SQLPLus object provides an interface for executing actions against a database connection
 */
public class SqlPlus {

	private Supplier<Connection> connectionFactory;
	private SessionIdMode sessionIDMode;
	private ConcurrentHashMap<Object, Session> id_currentSession = new ConcurrentHashMap<>();
	
	public SqlPlus(String url, String user, String pass) {
		this(url, user, pass, SessionIdMode.CURRENT_THREAD);
	}
	
	public SqlPlus(String url, String user, String pass, SessionIdMode idMode) {
		this(() -> {
			try {
				return DriverManager.getConnection(url, user, pass);
			}
			catch (Exception ex) {
				throw new ConfigurationException("Failed to connect to database", ex);
			}
		}, idMode);
	}

	public SqlPlus(DataSource src) {
		this(src, SessionIdMode.CURRENT_THREAD);
	}
	
	public SqlPlus(DataSource src, SessionIdMode idMode) {
		this(() -> {
			try {
				return src.getConnection();
			} catch (SQLException e) {
				throw new SqlRuntimeException(e);
			}
		}, idMode);
	}

	public SqlPlus(Supplier<Connection> factory) {
		this(factory, SessionIdMode.CURRENT_THREAD);
	}
	
	public SqlPlus(Supplier<Connection> factory, SessionIdMode idMode) {
		this.connectionFactory = factory;
		this.sessionIDMode = idMode;
	}

	public void setSessionIdMode(SessionIdMode mode) {
		this.sessionIDMode = mode;
	}
	
	public Object getCurrentSessionId() {
		switch (sessionIDMode) {
//		case DATA_SOURCE:
//			return hashCode();
		case CURRENT_THREAD:
		default:
			 return Thread.currentThread().getId();
		}
	}
	
	public void testConnection() {
		open(conn -> {
			// Throws if problems opening connection
		});
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

		Object currentSessionId = getCurrentSessionId();
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
			Session newSession = new Session(conn);
			id_currentSession.putIfAbsent(currentSessionId, newSession);
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
