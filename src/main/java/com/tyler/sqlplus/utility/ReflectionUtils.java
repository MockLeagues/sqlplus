package com.tyler.sqlplus.utility;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class ReflectionUtils {

	private static final Map<Field, Function<Object, Object>> FIELD_GET = new HashMap<>();
	private static final Map<Field, BiConsumer<Object, Object>> FIELD_SET = new HashMap<>();
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
		Function<Object, Object> getValue = null;
		if (FIELD_GET.containsKey(field)) {
			getValue = FIELD_GET.get(field);
		}
		else {
			try {
				try {
					Method getter = o.getClass().getDeclaredMethod("get" + field);
					getValue = (obj) -> { try { return getter.invoke(obj); } catch (Exception e) { throw new RuntimeException(e); } }; 
				} catch (NoSuchMethodException e1) {
					Method getter = o.getClass().getDeclaredMethod("is" + field);
					getValue = (obj) -> { try { return getter.invoke(obj); } catch (Exception e) { throw new RuntimeException(e); } };
				}
			}
			catch (Exception e) {
				getValue = (obj) -> {
					field.setAccessible(true);
					try {
						return field.get(obj);
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
				};
			}
			FIELD_GET.put(field, getValue);
		}
		
		return getValue.apply(o);
	}

	public static void set(String member, Object o, Object value) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
		set(o.getClass().getDeclaredField(member), o, value);
	}

	public static void set(Field field, Object o, Object value) throws IllegalArgumentException, IllegalAccessException {
		BiConsumer<Object, Object> setValue = null;
		
		if (FIELD_SET.containsKey(field)) {
			setValue = FIELD_SET.get(field);
		}
		else {
			try {
				String fieldName = field.getName();
				fieldName = String.valueOf(fieldName.charAt(0)).toUpperCase() + fieldName.substring(1);
				Method setter = o.getClass().getDeclaredMethod("set" + fieldName, value.getClass());
				setValue = (obj, val) -> { try { setter.invoke(obj, val); } catch (Exception e) { throw new RuntimeException(e); } }; 
			}
			catch (Exception e) {
				setValue = (obj, val) -> {
					field.setAccessible(true);
					try {
						field.set(obj, val);
					} catch (Exception e1) {
						throw new RuntimeException(e1);
					}
				};
			}
			FIELD_SET.put(field, setValue);
		}
		
		setValue.accept(o, value);
	}
	
}
