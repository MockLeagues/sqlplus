package com.tyler.sqlplus.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a related entity field / collection of entities with the sql which should be run to load it / them
 */
@Target({ ElementType.FIELD, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface LoadQuery {

	public String value();
	
	public String field() default "";
	
}
