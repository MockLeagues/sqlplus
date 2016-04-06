package com.tyler.sqlplus.conversion;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.tyler.sqlplus.annotation.Column;
import com.tyler.sqlplus.exception.MappingException;

public class Conversion {

	private final static Map<Class<?>, Function<Object, Object>> JAVA_FROMDB = new HashMap<>();
	static {
		JAVA_FROMDB.put(int.class, db -> ((Number)db).intValue());
		JAVA_FROMDB.put(Integer.class, db -> ((Number)db).intValue());
		JAVA_FROMDB.put(double.class, db -> ((Number)db).doubleValue());
		JAVA_FROMDB.put(Double.class, db -> ((Number)db).doubleValue());
		JAVA_FROMDB.put(char.class, db -> String.valueOf(db).charAt(0));
		JAVA_FROMDB.put(Character.class, db -> String.valueOf(db).charAt(0));
		JAVA_FROMDB.put(boolean.class, db -> (db + "").equalsIgnoreCase("y") || (db + "").equalsIgnoreCase("true") || (db + "").equalsIgnoreCase("1"));
		JAVA_FROMDB.put(Boolean.class, db -> (db + "").equalsIgnoreCase("y") || (db + "").equalsIgnoreCase("true") || (db + "").equalsIgnoreCase("1"));
		JAVA_FROMDB.put(String.class, db -> db + "");
		JAVA_FROMDB.put(Date.class, db -> db);
		JAVA_FROMDB.put(LocalDate.class, db -> ((java.sql.Date)db).toLocalDate());
	}
	
	@SuppressWarnings("unchecked")
	public static Object toJavaValue(Field target, Object dbValue) throws Exception {
		Column annot = target.getDeclaredAnnotation(Column.class);
		if (annot != null && annot.converter().length > 0) { // Custom conversion class
			return annot.converter()[0].newInstance().apply(dbValue);
		}
		return toJavaValue(target.getType(), dbValue);
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T toJavaValue(Class<T> targetClass, Object dbValue) {
		try {
			if (Enum.class.isAssignableFrom(targetClass)) { // Must convert over to specific enum type for this field
				return (T) targetClass.getDeclaredMethod("valueOf", String.class).invoke(null, dbValue.toString());
			}
			Function<Object, Object> converter = JAVA_FROMDB.get(targetClass);
			if (converter == null) {
				throw new MappingException("Unsupported java class type for DB conversion: " + targetClass.getName());
			}
			return (T) converter.apply(dbValue);
		}
		catch (Exception e) {
			throw new MappingException("Failed to convert result of type " + dbValue.getClass().getName() + " to type " + targetClass.getName(), e);
		}
	}
	
}
