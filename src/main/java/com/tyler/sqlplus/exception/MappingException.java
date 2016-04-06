package com.tyler.sqlplus.exception;

/**
 * Configuration-type exception which is thrown when there is a problem mapping a POJO class to a result set row
 */
public class MappingException extends RuntimeException {

	public MappingException(String msg) {
		super(msg);
	}
	
	public MappingException(String msg, Throwable e) {
		super(msg, e);
	}

	public MappingException(Exception e) {
		super(e);
	}
	
}
