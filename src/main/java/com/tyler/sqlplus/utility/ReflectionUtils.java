package com.tyler.sqlplus.utility;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.Map;

public final class ReflectionUtils {

	private ReflectionUtils() {}

	public static Class<?> getGenericType(Field f) {
		return (Class<?>) ((ParameterizedType)f.getGenericType()).getActualTypeArguments()[0];
	}
	
	public static Object get(String field, Object o) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		return get(o.getClass().getDeclaredField(field), o);
	}

	public static Object get(Field field, Object o) throws IllegalArgumentException, IllegalAccessException {
		try {
			String fieldName = field.getName();
			fieldName = "set" + String.valueOf(fieldName.charAt(0)).toUpperCase() + fieldName.substring(1).toLowerCase();
			try {
				return o.getClass().getDeclaredMethod("get" + fieldName).invoke(o);
			} catch (NoSuchMethodException e1) {
				return o.getClass().getDeclaredMethod("is" + fieldName).invoke(o);
			}
		} catch (Exception e) {
			field.setAccessible(true);
			return field.get(o);
		}
	}

	public static void set(String member, Object o, Object value) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		set(o.getClass().getDeclaredField(member), o, value);
	}

	public static void set(Field field, Object obj, Object value) throws IllegalArgumentException, IllegalAccessException {
		try {
			String fieldName = field.getName();
			String methodName = "set" + String.valueOf(fieldName.charAt(0)).toUpperCase() + fieldName.substring(1).toLowerCase();
			obj.getClass().getDeclaredMethod(methodName, field.getType()).invoke(obj, value);
		} catch (NoSuchMethodException | InvocationTargetException e) {
			field.setAccessible(true);
			field.set(obj, value);
		}
	}

	/**
	 * Takes a basic map of string -> string and maps it to a POJO instance.
	 * The keys in the map are expected to be the pojo field names and the values are the values for the fields
	 */
	public static <T> T toPOJO(Map<String, Object> map, Class<T> pojo) throws InstantiationException, IllegalAccessException, NoSuchFieldException {
		T instance = pojo.newInstance();
		for (Map.Entry<String, Object> e : map.entrySet()) {
			Field f = pojo.getDeclaredField(e.getKey());
			f.set(instance, e.getValue());
		}
		return instance;
	}
	
}
