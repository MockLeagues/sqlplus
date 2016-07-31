package com.tyler.sqlplus.exception;

public class SqlRuntimeException extends RuntimeException {

	public SqlRuntimeException(String msg, Throwable e) {
		super(msg, e);
	}

	public SqlRuntimeException(String string) {
		super(string);
	}

	public SqlRuntimeException(Exception e) {
		super(e);
	}
	
}
