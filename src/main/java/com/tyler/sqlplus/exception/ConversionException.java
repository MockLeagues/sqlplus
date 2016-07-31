package com.tyler.sqlplus.exception;

public class ConversionException extends RuntimeException {

	public ConversionException(String msg, Throwable t) {
		super(msg, t);
	}
	
	public ConversionException(String msg) {
		super(msg);
	}
	
}
