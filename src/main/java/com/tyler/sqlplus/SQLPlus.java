package com.tyler.sqlplus;

import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.function.Functions;
import com.tyler.sqlplus.proxy.TransactionalService;

import javax.sql.DataSource;
import java.io.IOException;
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
		this(new BasicDataSource(url, user, pass));
	}
	
	public SQLPlus(Supplier<Connection> connectionFactory) {
		this(new BasicDataSource(null, null, null) {
			
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
	public void transact(Functions.ThrowingConsumer<Session> action) {
		transactAndReturn(session -> {
			action.accept(session);
			return null;
		});
	}
	
	/**
	 * Executes a value-returning action against a database connection obtained from this instance's connection factory
	 * using the default isolation level
	 */
	public <T> T transactAndReturn(Functions.ThrowingFunction<Session, T> action) {
		return transactAndReturn(-1, action);
	}

	/**
	 * Executes a value-returning action against a database connection obtained from this instance's connection factory
	 * using the given transaction isolation level
	 */
	public <T> T transactAndReturn(int isolation, Functions.ThrowingFunction<Session, T> action) {

		Session currentSession = CURRENT_THREAD_SESSION.get();
		if (currentSession != null) {
			try {
				return action.apply(currentSession);
			}
			catch (Exception e) {
				throw new SQLRuntimeException(e);
			}
		}

		Session session = null;
		T result;
		try {
			session = new Session(dataSource.getConnection());
			if (isolation != -1) {
				session.conn.setTransactionIsolation(isolation);
			}
			session.conn.setAutoCommit(false);
			CURRENT_THREAD_SESSION.set(session);
			result = action.apply(session);
			session.conn.commit();
		}
		catch (Exception e) {
			CURRENT_THREAD_SESSION.remove();
			session.rollback();
			throw new SQLRuntimeException(e);
		}

		CURRENT_THREAD_SESSION.remove();
		try {
			session.close();
		} catch (IOException ex) {
			throw new SQLRuntimeException(ex);
		}
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
