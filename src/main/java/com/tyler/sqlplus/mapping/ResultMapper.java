package com.tyler.sqlplus.mapping;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.tyler.sqlplus.conversion.AttributeConverter;
import com.tyler.sqlplus.conversion.ConversionPolicy;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.utility.ReflectionUtils;

public interface ResultMapper<T> {

	public T map(ResultSet rs) throws SQLException;
	
	/**
	 * Creates a ResultMapper which will convert rows of a result set to objects of the given type using a default
	 * conversion policy
	 */
	public static <E> ResultMapper<E> forType(Class<E> type) throws SQLException {
		return forType(type, ConversionPolicy.DEFAULT);
	}
	
	public static <E> ResultMapper<E> forType(Class<E> type, ConversionPolicy conversionPolicy) throws SQLException {
		
		return new ResultMapper<E>() {

			private Set<Field> mappableFields;
			
			@Override
			public E map(ResultSet rs) throws SQLException {
				
				if (mappableFields == null) {
					mappableFields = findMappableFields(rs, type);
				}
				
				E instance;
				try {
					instance = type.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					throw new MappingException(
						"Could not construct instance of class " + type.getName() + ", verify it has a public no-args constructor");
				}
				
				for (Field mappableField : mappableFields) {
					AttributeConverter<?> converterForField = conversionPolicy.findConverter(mappableField.getType());
					if (converterForField != null) {
						Object fieldValue = converterForField.get(rs, mappableField.getName());
						try {
							ReflectionUtils.set(mappableField, instance, fieldValue);
						} catch (IllegalArgumentException | IllegalAccessException e) {
							throw new MappingException("Unable to set field value for field " + mappableField + " in class " + type.getName(), e);
						}
					}
				}
				
				return instance;
			}
			
		};
		
	}
	
	/**
	 * Detects which fields for the given class type can be mapped from the given result set. A field is considered
	 * mappable if its name is present as a column label (as pulled via 'getColumnLabel()')
	 */
	public static Set<Field> findMappableFields(ResultSet rs, Class<?> type) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		Set<Field> mappableFields = new HashSet<>();
		for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
			try {
				Field field = type.getDeclaredField(meta.getColumnLabel(col));
				mappableFields.add(field);
			} catch (NoSuchFieldException | SecurityException e) {
				// Not mappable
			}
		}
		return mappableFields;
	}
	
}
