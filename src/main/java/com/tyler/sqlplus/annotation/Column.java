package com.tyler.sqlplus.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Function;

/**
 * Provides very simply meta-data for mapping a pojo to database columns
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

	public String name() default "";
	
	@SuppressWarnings("rawtypes")
	public Class<? extends Function>[] converter() default {};
	
}
