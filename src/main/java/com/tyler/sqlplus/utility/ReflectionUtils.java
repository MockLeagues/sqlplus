package com.tyler.sqlplus.utility;

import static java.util.stream.Collectors.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.tyler.sqlplus.exception.ReflectionException;

public final class ReflectionUtils {

	// It is important to cache reflective data since it can be costly to lookup in bulk
	private static final Map<Field, Function<Object, Object>> FIELD_GETTER = new HashMap<>();
	private static final Map<Field, BiConsumer<Object, Object>> FIELD_SETTER = new HashMap<>();
	
	private ReflectionUtils() {}

	public static Object get(Field field, Object instance) {
		
		if (FIELD_GETTER.containsKey(field)) {
			return FIELD_GETTER.get(field).apply(instance);
		}
		
		Function<Object, Object> getValue = null;
		String capFieldName = capitalize(field.getName());
		try {
			Method getter = instance.getClass().getDeclaredMethod("get" + capFieldName);
			getValue = (obj) -> { try { return getter.invoke(obj); } catch (Exception e) { throw new ReflectionException(e); } }; 
		}
		catch (NoSuchMethodException e1) {
			try {
				Method getter = instance.getClass().getDeclaredMethod("is" + capFieldName);
				getValue = (obj) -> { try { return getter.invoke(obj); } catch (Exception e) { throw new ReflectionException(e); } };
			}
			catch (NoSuchMethodException e2) {
				field.setAccessible(true);
				getValue = (obj) -> { try { return field.get(obj); } catch (Exception e3) { throw new ReflectionException(e3); } };
			}
		}
		FIELD_GETTER.put(field, getValue);
		return getValue.apply(instance);
	}

	public static void set(Field field, Object instance, Object value) {
		
		if (FIELD_SETTER.containsKey(field)) {
			FIELD_SETTER.get(field).accept(instance, value);
		}
		else {
			BiConsumer<Object, Object> setValue = null;
			try {
				Method setter = instance.getClass().getDeclaredMethod("set" + capitalize(field.getName()));
				setValue = (obj, val) -> { try { setter.invoke(obj, val); } catch (Exception e) { throw new ReflectionException(e); } }; 
			}
			catch (NoSuchMethodException e) {
				field.setAccessible(true);
				setValue = (obj, val) -> { try { field.set(obj, val); } catch (Exception e1) { throw new ReflectionException(e1); } };
			}
			FIELD_SETTER.put(field, setValue);
			setValue.accept(instance, value);
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
			return firstWord + fieldWords.subList(1, fieldWords.size()).stream().map(ReflectionUtils::capitalize).collect(joining());
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
