package com.tyler.sqlplus.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that a method in a class should be wrapped inside of a SqlPlus transaction
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Transactional {

	/**
	 * Specifies the isolation level for the method's transaction. By default, the default
	 * isolation level for the current active driver will be used.
	 * Note that nested calls to transactional methods in the same thread stack will not
	 * override the original isolation level; only the first method in the thread which
	 * opened the transaction will set the level
	 */
	int isolation() default -1;

}
