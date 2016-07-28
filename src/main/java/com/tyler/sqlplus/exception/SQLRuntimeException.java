package com.tyler.sqlplus.exception;

public class SQLRuntimeException extends RuntimeException {

	public SQLRuntimeException(String msg, Throwable e) {
		super(msg, e);
	}

	public SQLRuntimeException(String string) {
		super(string);
	}

	public SQLRuntimeException(Exception e) {
		super(e);
	}
	
}
