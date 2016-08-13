package com.tyler.sqlplus.utility;

import static java.util.stream.Collectors.joining;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.functional.ReturningWork;
import com.tyler.sqlplus.functional.ThrowingBiConsumer;

/**
 * Utilities for working with {@link Field} objects
 */
public final class Fields {

	// It is important to cache reflective data since it is costly to lookup
	private static final Map<Field, ReturningWork<Object, Object>> FIELD_GETTER = new HashMap<>();
	private static final Map<Field, ThrowingBiConsumer<Object, Object>> FIELD_SETTER = new HashMap<>();
	
	private Fields() {}

	public static Object get(Field field, Object instance) {
		
		ReturningWork<Object, Object> getterFunction = FIELD_GETTER.computeIfAbsent(field, f -> {
			String capFieldName = capitalize(field.getName());
			try {
				Method getter = instance.getClass().getDeclaredMethod("get" + capFieldName);
				return getter::invoke; 
			}
			catch (NoSuchMethodException e1) {
				try {
					Method getter = instance.getClass().getDeclaredMethod("is" + capFieldName);
					return getter::invoke;
				}
				catch (NoSuchMethodException e2) {
					field.setAccessible(true);
					return field::get;
				}
			}
		});
		
		try {
			return getterFunction.doReturningWork(instance);
		} catch (Exception e) {
			throw new ReflectionException(e);
		}
	}

	public static void set(Field field, Object instance, Object value) {
		
		ThrowingBiConsumer<Object, Object> setterFunction = FIELD_SETTER.computeIfAbsent(field, f -> {
			try {
				Method setter = instance.getClass().getDeclaredMethod("set" + capitalize(field.getName()));
				return setter::invoke; 
			}
			catch (NoSuchMethodException e) {
				field.setAccessible(true);
				return field::set;
			}
		});
		
		try {
			setterFunction.accept(instance, value);
		} catch (Exception e) {
			throw new ReflectionException(e);
		}
	}

	/**
	 * Attempts to extract the field name referred to by the javabeans style getter / setter.
	 * Standard getter names begin with either 'get' or 'is'. Standard setter names begin with 'set'
	 */
	public static String extractFieldName(String methodName) {
		
		String fieldName;
		if (methodName.startsWith("get") || methodName.startsWith("set")) {
			fieldName = methodName.substring(3);
		}
		else if (methodName.startsWith("is")) {
			fieldName = methodName.substring(2);
		}
		else {
			throw new IllegalArgumentException(methodName + " does not conform to javabeans style accessor naming convetions");
		}
		
		return Character.toLowerCase(fieldName.charAt(0)) + fieldName.substring(1);
	}
	
	/**
	 * Converts a database field name in underscore format to the equivalent camel-case format.
	 * This method first converts the database field name to lowercase before converting to camelcase. For instance,
	 * both the columns "MY_FIELD" and "my_field" would get converted to the field name "myField"
	 */
	public static String underscoreToCamelCase(String dbField) {
		if (dbField == null || dbField.isEmpty()) {
			return "";
		}
		List<String> fieldWords = Arrays.asList(dbField.trim().toLowerCase().split("_"));
		String firstWord = fieldWords.get(0);
		if (fieldWords.size() == 1) {
			return firstWord;
		}
		else {
			return firstWord + fieldWords.subList(1, fieldWords.size()).stream().map(Fields::capitalize).collect(joining());
		}
	}
	
	public static String camelCaseToUnderscore(String javaField) {
		if (javaField == null || javaField.isEmpty()) {
			return "";
		}
		return javaField.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
	}
	
	private static String capitalize(String s) {
		return String.valueOf(s.charAt(0)).toUpperCase() + s.substring(1);
	}
	
}
