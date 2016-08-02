package com.tyler.sqlplus.exception;

public class ReflectionException extends RuntimeException {

	public ReflectionException(String msg) {
		super(msg);
	}
	
	public ReflectionException(Exception ex) {
		super(ex);
	}
	
}
