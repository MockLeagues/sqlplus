package com.tyler.sqlplus.utility;

import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.functional.ReturningWork;
import com.tyler.sqlplus.functional.ThrowingBiConsumer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

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

	public static String underscoreToCamelCase(String underscoreStr) {
		StringBuilder newStr = new StringBuilder();

		char[] chars = underscoreStr.toCharArray();
		for (int i = 0; i < chars.length; i++) {

			char thisChar = chars[i];
			if (thisChar == '_') {
				continue;
			}

			if (i > 0 && chars[i - 1] == '_') {
				newStr.append(Character.toUpperCase(thisChar));
			} else {
				newStr.append(Character.toLowerCase(thisChar));
			}
		}

		return newStr.toString();
	}

	public static String camelCaseToUnderscore(String camelCaseStr) {
		StringBuilder newStr = new StringBuilder();

		char[] chars = camelCaseStr.toCharArray();
		for (int i = 0; i < chars.length; i++) {

			char thisChar = chars[i];
			if (Character.isUpperCase(thisChar) && i > 0) {
				newStr.append('_');
			}
			newStr.append(thisChar);
		}

		return newStr.toString().toUpperCase();
	}

	private static String capitalize(String s) {
		return String.valueOf(s.charAt(0)).toUpperCase() + s.substring(1);
	}
	
}
