package com.tyler.sqlplus;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tyler.sqlplus.conversion.AttributeConverter;
import com.tyler.sqlplus.conversion.ConversionPolicy;
import com.tyler.sqlplus.exception.POJOBindException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.utility.ReflectionUtils;

/**
 * Defines the contract for a class which maps a result set row to an object of type <T>
 */
public interface ResultMapper<T> {

	public T map(ResultSet rs) throws SQLException;
	
	/**
	 * Creates a result mapper which maps rows to string arrays
	 */
	public static ResultMapper<String[]> forStringArray() {
		return rs -> {
			List<String> row = new ArrayList<>();
			ResultSetMetaData meta = rs.getMetaData();
			for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
				row.add(rs.getString(col));
			}
			return row.toArray(new String[row.size()]);
		};
	}
	
	/**
	 * Creates a result mapper which maps rows to hash maps
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Map<String, String>> ResultMapper<Map<String, String>> forMap() {
		return forMap(HashMap.class);
	}
	
	/**
	 * Creates a result mapper which maps rows to java Map objects of the given implementation
	 */
	public static <T extends Map<String, String>> ResultMapper<Map<String, String>> forMap(Class<T> impl) {
		return rs -> {
			Map<String, String> row;
			if (impl == Map.class) {
				row = new HashMap<>();
			}
			try {
				row = impl.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new SQLRuntimeException("Could not instantiate instance of map implementation " + impl.getName());
			}
			
			ResultSetMetaData meta = rs.getMetaData();
			for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
				row.put(meta.getColumnLabel(col), rs.getString(col));
			}
			
			return row;
		};
	};
	
	/**
	 * Creates a result mapper which will convert rows of a result set to objects of the given POJO class
	 */
	public static <E> ResultMapper<E> forType(Class<E> type) throws SQLException {
		return forType(type, ConversionPolicy.DEFAULT);
	}
	
	/**
	 * Creates a result mapper which will convert rows of a result set to objects of the given POJO class using the given conversion policy
	 */
	@SuppressWarnings("unchecked")
	public static <E> ResultMapper<E> forType(Class<E> type, ConversionPolicy conversionPolicy) throws SQLException {
		
		if (Map.class.isAssignableFrom(type)) {
			return (ResultMapper<E>) forMap((Class<? extends Map<String, String>>) type);
		}
		
		if (String[].class == type) {
			return (ResultMapper<E>) forStringArray();
		}
		
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
					throw new POJOBindException(
						"Could not construct instance of class " + type.getName() + ", verify it has a public no-args constructor");
				}
				
				for (Field mappableField : mappableFields) {
					AttributeConverter<?> converterForField = conversionPolicy.findConverter(mappableField.getType());
					if (converterForField != null) {
						Object fieldValue = converterForField.get(rs, mappableField.getName());
						try {
							ReflectionUtils.set(mappableField, instance, fieldValue);
						} catch (IllegalArgumentException | IllegalAccessException e) {
							throw new POJOBindException("Unable to set field value for field " + mappableField + " in class " + type.getName(), e);
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
		Set<Field> mappableFields = new HashSet<>();
		ResultSetMetaData meta = rs.getMetaData();
		for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
			String label = meta.getColumnLabel(col);
			try {
				Field mappableField = type.getDeclaredField(label);
				mappableFields.add(mappableField);
			} catch (NoSuchFieldException | SecurityException e) {
				// Not mappable
			}
		}
		return mappableFields;
	}
	
}
