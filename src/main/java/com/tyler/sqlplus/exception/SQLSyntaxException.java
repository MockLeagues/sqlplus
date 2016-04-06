package com.tyler.sqlplus.exception;

import java.sql.SQLException;

public class SQLSyntaxException extends RuntimeException {

	public SQLSyntaxException(String msg, Throwable e) {
		super(msg, e);
	}

	public SQLSyntaxException(String string) {
		super(string);
	}

	public SQLSyntaxException(SQLException e) {
		super(e);
	}
	
}
