package com.tyler.sqlplus;

/**
 * Defines methods for identifying the 'current' session for any given SqlPlus instance.
 */
public enum SessionIDMode {

	/**
	 * In current thread mode, sessions are associated with the threads that spawned them. Any call to retrieve the current
	 * session will therefore be the session associated with the currente executing thread.
	 */
	CURRENT_THREAD,
	
	/**
	 * In data source mode, sessions are associated with data sources themselves. In this mode there can only ever be 1 active
	 * session per data source, regardless of how many threads might be accessing it.
	 *
	*DATA_SOURCE;
	*/
	
}
