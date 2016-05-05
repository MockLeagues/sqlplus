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
	
	public static Object get(String fieldName, Object o) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		return get(o.getClass().getDeclaredField(fieldName), o);
	}
	
	public static Object get(Field field, Object o) throws IllegalArgumentException, IllegalAccessException, SecurityException {
		
		// See if we can pull a getter from our cache first
		if (FIELD_GETTER.containsKey(field)) {
			return FIELD_GETTER.get(field).apply(o);
		}
		
		Function<Object, Object> getValue = null;
		String capFieldName = capitalize(field.getName());
		try {
			Method getter = o.getClass().getDeclaredMethod("get" + capFieldName);
			getValue = (obj) -> { try { return getter.invoke(obj); } catch (Exception e) { throw new RuntimeException(e); } }; 
		}
		catch (NoSuchMethodException e1) {
			try {
				Method getter = o.getClass().getDeclaredMethod("is" + capFieldName);
				getValue = (obj) -> { try { return getter.invoke(obj); } catch (Exception e) { throw new RuntimeException(e); } };
			}
			catch (NoSuchMethodException e2) {
				getValue = (obj) -> {
					field.setAccessible(true);
					try { return field.get(obj); } catch (Exception e3) { throw new RuntimeException(e3); }
				};
			}
		}
		FIELD_GETTER.put(field, getValue);
		return getValue.apply(o);
	}

	public static void set(String member, Object o, Object value) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		set(o.getClass().getDeclaredField(member), o, value);
	}

	public static void set(Field field, Object o, Object value) throws IllegalArgumentException, IllegalAccessException {
		
		if (FIELD_SETTER.containsKey(field)) {
			FIELD_SETTER.get(field).accept(o, value);
		}
		else {
			BiConsumer<Object, Object> setValue = null;
			try {
				Method setter = o.getClass().getDeclaredMethod("set" + capitalize(field.getName()));
				setValue = (obj, val) -> { try { setter.invoke(obj, val); } catch (Exception e) { throw new RuntimeException(e); } }; 
			}
			catch (NoSuchMethodException e) {
				setValue = (obj, val) -> {
					field.setAccessible(true);
					try { field.set(obj, val); } catch (Exception e1) { throw new RuntimeException(e1); }
				};
			}
			FIELD_SETTER.put(field, setValue);
			setValue.accept(o, value);
		}
	}
	
	private static String capitalize(String s) {
		return String.valueOf(s.charAt(0)).toUpperCase() + s.substring(1);
	}
	
}
