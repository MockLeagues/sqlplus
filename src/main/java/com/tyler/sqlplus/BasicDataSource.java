package com.tyler.sqlplus;

import javax.sql.DataSource;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * A basic, bare-bones implementation of a {@link DataSource} which provides connection from a username, password, and database URL.
 * <br/>
 * This data source makes calls directly to the {@link DriverManager}, so no connection pooling will take place. Therefore, this data source
 * should only be used for initial development
 */
public class BasicDataSource implements DataSource {

	private static final int DEFAULT_LOGIN_TIMEOUT_SECONDS = 3;
	private static final PrintStream DEFAULT_LOGGER_PRINT_STREAM = System.out;

	private PrintWriter logWriter;
	private int loginTimeoutSeconds;
	private String url;
	private String username;
	private String password;
	private String driverClass;

	private BasicDataSource() {
		logWriter = new PrintWriter(DEFAULT_LOGGER_PRINT_STREAM);
		loginTimeoutSeconds = DEFAULT_LOGIN_TIMEOUT_SECONDS;
	}

	public BasicDataSource(String url, String username, String password) {
		this();
		this.url = url;
		this.username = username;
		this.password = password;
	}

	public String getUrl() {
		return url;
	}
	
	public BasicDataSource setUrl(String url) {
		this.url = url;
		return this;
	}
	
	public String getUsername() {
		return username;
	}
	
	public BasicDataSource setUsername(String username) {
		this.username = username;
		return this;
	}
	
	public String getPassword() {
		return password;
	}
	
	public BasicDataSource setPassword(String password) {
		this.password = password;
		return this;
	}
	
	public String getDriverClass() {
		return driverClass;
	}
	
	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}
	
	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return logWriter;
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		this.logWriter = out;
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		this.loginTimeoutSeconds = seconds;
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return loginTimeoutSeconds;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException(getClass() + " does not support parent loggers");
	}

	@Override
	public <T> T unwrap(Class<T> classToUnwrap) throws SQLException {
		throw new UnsupportedOperationException(getClass() + " does not support unwrap()");
	}

	@Override
	public boolean isWrapperFor(Class<?> c) throws SQLException {
		return false;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, username, password);
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		return DriverManager.getConnection(url, username, password);
	}

}
