package com.tyler.sqlplus.exception;

public class SQLSyntaxException extends RuntimeException {

	public SQLSyntaxException(String msg, Throwable e) {
		super(msg, e);
	}

	public SQLSyntaxException(String string) {
		super(string);
	}
	
}
