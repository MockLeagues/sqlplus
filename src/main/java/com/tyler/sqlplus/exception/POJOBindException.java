package com.tyler.sqlplus.exception;

/**
 * Exception which is thrown when there is a problem mapping a a result set row to a POJO class or
 * binding POJO params to query 
 */
public class POJOBindException extends RuntimeException {

	public POJOBindException(String msg) {
		super(msg);
	}
	
	public POJOBindException(String msg, Throwable e) {
		super(msg, e);
	}

	public POJOBindException(Exception e) {
		super(e);
	}
	
}
