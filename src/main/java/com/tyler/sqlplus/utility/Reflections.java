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

public final class Reflections {

	// It is important to cache reflective data since it is costly to lookup
	private static final Map<Field, ReturningWork<Object, Object>> FIELD_GETTER = new HashMap<>();
	private static final Map<Field, ThrowingBiConsumer<Object, Object>> FIELD_SETTER = new HashMap<>();
	
	private Reflections() {}

	public static Object get(Field field, Object instance) {
		
		ReturningWork<Object, Object> getterFunction;
		
		if (FIELD_GETTER.containsKey(field)) {
			getterFunction = FIELD_GETTER.get(field);
		}
		else {
			String capFieldName = capitalize(field.getName());
			try {
				Method getter = instance.getClass().getDeclaredMethod("get" + capFieldName);
				getterFunction = getter::invoke; 
			}
			catch (NoSuchMethodException e1) {
				try {
					Method getter = instance.getClass().getDeclaredMethod("is" + capFieldName);
					getterFunction = getter::invoke;
				}
				catch (NoSuchMethodException e2) {
					field.setAccessible(true);
					getterFunction = field::get;
				}
			}
			FIELD_GETTER.put(field, getterFunction);
		}
		
		try {
			return getterFunction.doReturningWork(instance);
		} catch (Exception e) {
			throw new ReflectionException(e);
		}
	}

	public static void set(Field field, Object instance, Object value) {
		
		ThrowingBiConsumer<Object, Object> setterFunction;
		
		if (FIELD_SETTER.containsKey(field)) {
			setterFunction = FIELD_SETTER.get(field);
		}
		else {
			try {
				Method setter = instance.getClass().getDeclaredMethod("set" + capitalize(field.getName()));
				setterFunction = setter::invoke; 
			}
			catch (NoSuchMethodException e) {
				field.setAccessible(true);
				setterFunction = field::set;
			}
			FIELD_SETTER.put(field, setterFunction);
		}
		
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
			return firstWord + fieldWords.subList(1, fieldWords.size()).stream().map(Reflections::capitalize).collect(joining());
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
