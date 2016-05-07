package com.tyler.sqlplus.serialization;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.tyler.sqlplus.exception.MappingException;

public class Converter {

	private final static Map<Class<?>, Function<?, String>> DEFAULT_SERIALIZERS = new HashMap<>();
	private final static Map<Class<?>, Function<String, ?>> DEFAULT_DESERIALIZERS = new HashMap<>();
	private final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	
	public static <T> void setDefaultConversion(Class<T> type, Function<T, String> serializer, Function<String, T> deserializer) {
		DEFAULT_SERIALIZERS.put(type, serializer);
		DEFAULT_DESERIALIZERS.put(type, deserializer);
	}
	
	static {
		setDefaultConversion(int.class,       String::valueOf,     Integer::parseInt);
		setDefaultConversion(Integer.class,   String::valueOf,     Integer::parseInt);
		setDefaultConversion(short.class,     String::valueOf,     Short::parseShort);
		setDefaultConversion(Short.class,     String::valueOf,     Short::parseShort);
		setDefaultConversion(long.class,      String::valueOf,     Long::parseLong);
		setDefaultConversion(Long.class,      String::valueOf,     Long::parseLong);
		setDefaultConversion(float.class,     String::valueOf,     Float::parseFloat);
		setDefaultConversion(Float.class,     String::valueOf,     Float::parseFloat);
		setDefaultConversion(double.class,    String::valueOf,     Double::parseDouble);
		setDefaultConversion(Double.class,    String::valueOf,     Double::parseDouble);
		setDefaultConversion(char.class,      String::valueOf,     db -> db.charAt(0));
		setDefaultConversion(Character.class, String::valueOf,     db -> db.charAt(0));
		setDefaultConversion(LocalDate.class, String::valueOf,     LocalDate::parse);
		setDefaultConversion(String.class,    String::valueOf,     String::valueOf);
		setDefaultConversion(boolean.class,   b -> b ? "1" : "0",  db -> db.equalsIgnoreCase("y") || db.equalsIgnoreCase("true") || db.equalsIgnoreCase("1"));
		setDefaultConversion(Boolean.class,   b -> b ? "1" : "0",  db -> db.equalsIgnoreCase("y") || db.equalsIgnoreCase("true") || db.equalsIgnoreCase("1"));
		
		setDefaultConversion(
			Date.class,
			d -> new SimpleDateFormat(DEFAULT_DATE_FORMAT).format(d),
			db -> {
				try {
					return new SimpleDateFormat(DEFAULT_DATE_FORMAT).parse(db);
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			});
	}
	
	private Map<Class<?>, Function<?, String>> serializers = new HashMap<>();
	private Map<Class<?>, Function<String, ?>> deserializers = new HashMap<>();
	
	public <T> void setConversion(Class<T> type, Function<T, String> serialize, Function<String, T> deserialize) {
		this.serializers.put(type, serialize);
		this.deserializers.put(type, deserialize);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T deserialize(Class<T> targetType, String dbValue) {
		
		if (dbValue == null) {
			return null;
		}
		
		if (deserializers.containsKey(targetType)) {
			Function<String, T> deserializer = (Function<String, T>) deserializers.get(targetType);
			return deserializer.apply(dbValue);
		}
		
		try {
			
			if (Enum.class.isAssignableFrom(targetType)) {

				Method valueOf = targetType.getDeclaredMethod("valueOf", String.class);
				Function<String, T> deserializer = s -> {
					try {
						return (T) valueOf.invoke(null, s);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				};
				
				deserializers.put(targetType, deserializer);
				return deserializer.apply(dbValue);
			}
			
			Function<String, ?> defaultDeserializer = DEFAULT_DESERIALIZERS.get(targetType);
			if (defaultDeserializer == null) {
				throw new MappingException("Unsupported java class type for DB de-serialization: " + targetType.getName());
			}
			
			deserializers.put(targetType, defaultDeserializer);
			return (T) defaultDeserializer.apply(dbValue);
		}
		catch (Exception e) {
			throw new MappingException("Failed to convert result of type " + dbValue.getClass().getName() + " to type " + targetType.getName(), e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T> String serialize(T o) {
		
		if (o == null) {
			return null;
		}
		
		Class<?> type = o.getClass();
		if (serializers.containsKey(type)) {
			Function<T, String> serializer = (Function<T, String>) serializers.get(type);
			return serializer.apply(o);
		}
		
		if (o instanceof Enum) {
			Function<T, String> serializer = Object::toString;
			serializers.put(type, serializer);
			return serializer.apply(o);
		}
		
		Function<T, String> defaultSerializer = (Function<T, String>) DEFAULT_SERIALIZERS.get(o.getClass());
		if (defaultSerializer == null) {
			throw new MappingException("Unsupported java class type for DB serialization: " + type.getName());
		}
		serializers.put(type, defaultSerializer);
		return defaultSerializer.apply(o);
	}
	
}
