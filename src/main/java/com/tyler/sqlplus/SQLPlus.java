package com.tyler.sqlplus;

import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.function.Functions;
import com.tyler.sqlplus.function.ReturningWork;
import com.tyler.sqlplus.function.Work;
import com.tyler.sqlplus.proxy.TransactionalService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;

/**
 * This class is the primary entry point to the SQLPlus API.
 * 
 * An instance of a SQLPLus object provides an interface for executing actions against a database connection
 */
public class SQLPlus {

	private static final ThreadLocal<Session> CURRENT_THREAD_SESSION = new ThreadLocal<>();
	
	private DataSource dataSource;

	@SuppressWarnings("unused")
	private SQLPlus() {}
	
	public SQLPlus(String url, String user, String pass) {
		this(new BasicDataSource().setUrl(url).setUsername(user).setPassword(pass));
	}
	
	public SQLPlus(Supplier<Connection> connectionFactory) {
		this(new BasicDataSource() {
			
			@Override
			public Connection getConnection(String user, String pass) {
				throw new UnsupportedOperationException("This data source cannot supply connections for an arbitrary user");
			}

			@Override
			public Connection getConnection() {
				return connectionFactory.get();
			}
			
		});
	}
	
	public SQLPlus(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public <T> T createService(Class<T> klass) throws InstantiationException, IllegalAccessException {
		return TransactionalService.create(klass, this);
	}

	public Session getCurrentSession() {
		Session currentSession = CURRENT_THREAD_SESSION.get();
		if (currentSession == null) {
			throw new IllegalStateException("No session is bound to the current thread");
		}
		return currentSession;
	}

	/**
	 * Executes an action inside of a single database transaction using the default isolation level.
	 * 
	 * If any exceptions are thrown, the transaction is immediately rolled back
	 */
	public void transact(Work<Session> action) {
		query(session -> {
			action.doWork(session);
			return null;
		});
	}

	/**
	 * Executes an action inside of a single database transaction using the given isolation level.
	 *
	 * If any exceptions are thrown, the transaction is immediately rolled back
	 */
	public void transact(int isolation, Work<Session> action) {
		query(isolation, session -> {
			action.doWork(session);
			return null;
		});
	}
	
	/**
	 * Executes a value-returning action against a database connection obtained from this instance's connection factory
	 * using the default isolation level
	 */
	public <T> T query(ReturningWork<Session, T> action) {
		return query(-1, action);
	}

	/**
	 * Executes a value-returning action against a database connection obtained from this instance's connection factory
	 * using the given transaction isolation level
	 */
	public <T> T query(int isolation, ReturningWork<Session, T> action) {

		Session currentSession = CURRENT_THREAD_SESSION.get();
		if (currentSession != null) {
			try {
				return action.doReturningWork(currentSession);
			} catch (Exception e) {
				throw new SQLRuntimeException(e);
			}
		}

		Connection conn = null;
		T result;

		try {
			conn = dataSource.getConnection();
			if (isolation != -1) {
				conn.setTransactionIsolation(isolation);
			}
			conn.setAutoCommit(false);
			Session newSession = new Session(conn);
			CURRENT_THREAD_SESSION.set(newSession);
			result = action.doReturningWork(newSession);
			conn.commit();
		}
		catch (Exception e) {
			CURRENT_THREAD_SESSION.remove();
			if (conn != null) {
				try {
					conn.rollback();
					conn.close();
				} catch (SQLException e2) {
					throw new SQLRuntimeException(e2);
				}
			}
			throw new SQLRuntimeException(e);
		}

		CURRENT_THREAD_SESSION.remove();
		Functions.runSQL(conn::close);
		return result;
	}
	
	public int[] batchExec(String... stmts) {
		try (Connection conn = dataSource.getConnection()) {
			Statement s = conn.createStatement();
			for (String sql : stmts) {
				s.addBatch(sql);
			}
			return s.executeBatch();
		}
		catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}

}
