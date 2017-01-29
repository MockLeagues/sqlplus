package com.tyler.sqlplus.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DAOUpdate {

	enum ReturnInfo {GENERATED_KEYS, AFFECTED_ROWS, NONE; }

	String value();
	
	ReturnInfo returnInfo() default ReturnInfo.NONE;
	
}
