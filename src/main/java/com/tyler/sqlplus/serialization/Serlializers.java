package com.tyler.sqlplus.serialization;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.tyler.sqlplus.annotation.Column;
import com.tyler.sqlplus.exception.MappingException;

public class Serlializers {

	private final static Map<Class<?>, Function<?, String>> TYPE_SER = new HashMap<>();
	private final static Map<Class<?>, Function<String, ?>> TYPE_DESER = new HashMap<>();
	
	public static <T> void setDefaultSerializer(Class<T> type, Function<T, String> serializer) {
		TYPE_SER.put(type, serializer);
	}
	
	public static <T> void setDefaultDeserializer(Class<T> type, Function<String, T> deserializer) {
		TYPE_DESER.put(type, deserializer);
	}
	
	static {
		
		setDefaultSerializer(int.class, i -> i + "");
		setDefaultDeserializer(int.class, Integer::parseInt);
		
		setDefaultSerializer(Integer.class, i -> i + "");
		setDefaultDeserializer(Integer.class, Integer::parseInt);
		
		setDefaultSerializer(float.class, f -> f + "");
		setDefaultDeserializer(float.class, Float::parseFloat);

		setDefaultSerializer(Float.class, f -> f + "");
		setDefaultDeserializer(Float.class, Float::parseFloat);
		
		setDefaultSerializer(double.class, d -> d + "");
		setDefaultDeserializer(double.class, Double::parseDouble);
		
		setDefaultSerializer(Double.class, d -> d + "");
		setDefaultDeserializer(Double.class, Double::parseDouble);
		
		setDefaultSerializer(char.class, c -> c + "");
		setDefaultDeserializer(char.class, db -> db.charAt(0));
		
		setDefaultSerializer(Character.class, c -> c + "");
		setDefaultDeserializer(Character.class, db -> db.charAt(0));
		
		setDefaultSerializer(boolean.class, b -> b ? "1" : "0");
		setDefaultDeserializer(boolean.class, db -> db.equalsIgnoreCase("y") || db.equalsIgnoreCase("true") || db.equalsIgnoreCase("1"));
		
		setDefaultSerializer(Boolean.class, b -> b ? "1" : "0");
		setDefaultDeserializer(Boolean.class, db -> db.equalsIgnoreCase("y") || db.equalsIgnoreCase("true") || db.equalsIgnoreCase("1"));
		
		setDefaultSerializer(LocalDate.class, d -> d.toString());
		setDefaultDeserializer(LocalDate.class, LocalDate::parse);
		
		setDefaultSerializer(Date.class, d -> d.toString());
		setDefaultDeserializer(Date.class, db -> new Date(java.sql.Date.valueOf(db).getTime()));
		
		setDefaultSerializer(String.class, s -> s);
		setDefaultDeserializer(String.class, s -> s);
	}
	
	@SuppressWarnings("unchecked")
	public static Object deserialize(Field target, String dbValue) throws Exception {
		Column annot = target.getDeclaredAnnotation(Column.class);
		if (annot != null && annot.converter().length > 0) { // Custom conversion class
			return annot.converter()[0].newInstance().apply(dbValue);
		}
		return deserialize(target.getType(), dbValue);
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T deserialize(Class<T> targetClass, String dbValue) {
		if (dbValue == null) return null;
		try {
			if (Enum.class.isAssignableFrom(targetClass)) { // Must convert over to specific enum type for this field
				return (T) targetClass.getDeclaredMethod("valueOf", String.class).invoke(null, dbValue.toString());
			}
			Function<String, ?> converter = TYPE_DESER.get(targetClass);
			if (converter == null) {
				throw new MappingException("Unsupported java class type for DB conversion: " + targetClass.getName());
			}
			return (T) converter.apply(dbValue);
		}
		catch (Exception e) {
			throw new MappingException("Failed to convert result of type " + dbValue.getClass().getName() + " to type " + targetClass.getName(), e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> String serialize(T o) {
		if (o == null) {
			return null;
		}
		else if (o instanceof Enum) {
			return o.toString();
		}
		else {
			Function<T, String> ser = (Function<T, String>) TYPE_SER.get(o.getClass());
			return ser.apply(o);
		}
	}
	
}
