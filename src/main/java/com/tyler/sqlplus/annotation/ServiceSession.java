package com.tyler.sqlplus.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field in a service class to inject the working session into when using instrumented service classes.
 * Instrumented service classes are created via a call to createTransactionAwareService() on a {@link SqlPlus} object. Instances
 * of this classes will execute their methods inside of a transaction.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ServiceSession {}
