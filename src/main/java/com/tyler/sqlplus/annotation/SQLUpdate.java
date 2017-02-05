package com.tyler.sqlplus.annotation;

import com.tyler.sqlplus.keyprovider.KeyProvider;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SQLUpdate {

	enum ReturnInfo { GENERATED_KEYS, AFFECTED_ROWS }

	String value();

	int isolation() default -1;

	ReturnInfo returnInfo() default ReturnInfo.AFFECTED_ROWS;

	/**
	 * Defines a key provider to use for creating primary keys for new database entities
	 */
	Class<? extends KeyProvider> keyProvider() default KeyProvider.VoidKeyProvider.class;

	/**
	 * Defines SQL to execute to retrieve a new key. Either this or keyProvider() may be defined, not both
	 */
	String keyQuery() default "";

}
