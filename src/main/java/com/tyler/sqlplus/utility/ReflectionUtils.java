package com.tyler.sqlplus.utility;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class ReflectionUtils {

	// It is important to cache reflective data since it can be costly to lookup in bulk
	private static final Map<Field, Function<Object, Object>> FIELD_GETTER = new HashMap<>();
	private static final Map<Field, BiConsumer<Object, Object>> FIELD_SETTER = new HashMap<>();
	private static final Map<Field, Class<?>> FIELD_GENERIC = new HashMap<>();
	
	private ReflectionUtils() {}

	public static Class<?> getGenericType(Field f) {
		if (FIELD_GENERIC.containsKey(f)) {
			return FIELD_GENERIC.get(f);
		}
		else {
			Class<?> genericType = (Class<?>) ((ParameterizedType)f.getGenericType()).getActualTypeArguments()[0];
			FIELD_GENERIC.put(f, genericType);
			return genericType;
		}
	}
	
	public static Object get(String fieldName, Object instance) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		return get(instance.getClass().getDeclaredField(fieldName), instance);
	}
	
	public static Object get(Field field, Object instance) throws IllegalArgumentException, IllegalAccessException, SecurityException {
		
		if (FIELD_GETTER.containsKey(field)) {
			return FIELD_GETTER.get(field).apply(instance);
		}
		
		Function<Object, Object> getValue = null;
		String capFieldName = capitalize(field.getName());
		try {
			Method getter = instance.getClass().getDeclaredMethod("get" + capFieldName);
			getValue = (obj) -> { try { return getter.invoke(obj); } catch (Exception e) { throw new RuntimeException(e); } }; 
		}
		catch (NoSuchMethodException e1) {
			try {
				Method getter = instance.getClass().getDeclaredMethod("is" + capFieldName);
				getValue = (obj) -> { try { return getter.invoke(obj); } catch (Exception e) { throw new RuntimeException(e); } };
			}
			catch (NoSuchMethodException e2) {
				field.setAccessible(true);
				getValue = (obj) -> { try { return field.get(obj); } catch (Exception e3) { throw new RuntimeException(e3); } };
			}
		}
		FIELD_GETTER.put(field, getValue);
		return getValue.apply(instance);
	}

	public static void set(String member, Object instance, Object value) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		set(instance.getClass().getDeclaredField(member), instance, value);
	}

	public static void set(Field field, Object instance, Object value) throws IllegalArgumentException, IllegalAccessException {
		
		if (FIELD_SETTER.containsKey(field)) {
			FIELD_SETTER.get(field).accept(instance, value);
		}
		else {
			BiConsumer<Object, Object> setValue = null;
			try {
				Method setter = instance.getClass().getDeclaredMethod("set" + capitalize(field.getName()));
				setValue = (obj, val) -> { try { setter.invoke(obj, val); } catch (Exception e) { throw new RuntimeException(e); } }; 
			}
			catch (NoSuchMethodException e) {
				field.setAccessible(true);
				setValue = (obj, val) -> { try { field.set(obj, val); } catch (Exception e1) { throw new RuntimeException(e1); } };
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
	
	private static String capitalize(String s) {
		return String.valueOf(s.charAt(0)).toUpperCase() + s.substring(1);
	}
	
}
