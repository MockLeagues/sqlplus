package com.tyler.sqlplus.exception;

public class ConfigurationException extends RuntimeException {

	public ConfigurationException(String msg, Throwable e) {
		super(msg, e);
	}

	public ConfigurationException() {}
	
}
