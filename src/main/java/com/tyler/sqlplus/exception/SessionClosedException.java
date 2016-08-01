package com.tyler.sqlplus.exception;

/**
 * Thrown when an illegal action is taken against a closed session
 */
public class SessionClosedException extends RuntimeException {

	public SessionClosedException() {}
	
	public SessionClosedException(String msg) {
		super(msg);
	}
	
}
