package com.tyler.sqlplus;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.tyler.sqlplus.conversion.AttributeConverter;
import com.tyler.sqlplus.conversion.ConversionPolicy;
import com.tyler.sqlplus.exception.POJOBindException;
import com.tyler.sqlplus.exception.SqlRuntimeException;
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
	public static <T extends Map<String, Object>> ResultMapper<T> forMap() {
		return (ResultMapper<T>) forMap(HashMap.class);
	}
	
	/**
	 * Creates a result mapper which maps rows to java Map objects of the given implementation
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Map<String, Object>> ResultMapper<T> forMap(Class<T> impl) {
		return rs -> {
			Map<String, Object> row;
			if (impl == Map.class) {
				row = new HashMap<>();
			}
			try {
				row = impl.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new SqlRuntimeException("Could not instantiate instance of map implementation " + impl.getName());
			}
			
			ResultSetMetaData meta = rs.getMetaData();
			for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
				row.put(meta.getColumnLabel(col), rs.getObject(col));
			}
			
			return (T) row;
		};
	};

	@SuppressWarnings("unchecked")
	public static <E> ResultMapper<E> forType(Class<E> type) throws SQLException {
		return forType(type, Collections.EMPTY_MAP);
	}
	
	public static <E> ResultMapper<E> forType(Class<E> type, Map<String, String> rsCol_fieldName) throws SQLException {
		return forType(type, ConversionPolicy.DEFAULT, rsCol_fieldName);
	}
	
	public static <E> ResultMapper<E> forType(Class<E> type, ConversionPolicy conversionPolicy, Map<String, String> rsCol_fieldName) {

		// Since we iterate over the POJO class fields when mapping them from the result set, we need to invert the given map
		// to ensure the keys are the POJO class field names, not the result set columns
		Map<String, String> fieldName_rsCol = new HashMap<>();
		rsCol_fieldName.forEach((rsCol, fieldName) -> fieldName_rsCol.put(fieldName, rsCol));
		
		return new ResultMapper<E>() {

			private Set<Field> mappableFields;
			
			@Override
			public E map(ResultSet rs) throws SQLException {
				
				if (mappableFields == null) {
					mappableFields = determineMappableFields(rs, type, rsCol_fieldName);
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
					
					String nameOfFieldToMap = mappableField.getName();
					String rsColumnName;
					if (fieldName_rsCol.containsKey(nameOfFieldToMap)) {
						rsColumnName = fieldName_rsCol.get(nameOfFieldToMap);
					}
					else {
						rsColumnName = nameOfFieldToMap;
					}
					
					Object fieldValue = converterForField.get(rs, rsColumnName);
					try {
						ReflectionUtils.set(mappableField, instance, fieldValue);
					} catch (IllegalArgumentException | IllegalAccessException e) {
						throw new POJOBindException("Unable to set field value for field " + mappableField, e);
					}
				}
				
				return instance;
			}

		};
		
	}

	@SuppressWarnings("unchecked")
	public static Set<Field> determineMappableFields(ResultSet rs, Class<?> type) throws SQLException {
		return determineMappableFields(rs, type, Collections.EMPTY_MAP);
	}
	
	public static Set<Field> determineMappableFields(ResultSet rs, Class<?> type, Map<String, String> rsColName_fieldName) throws SQLException {
		
		Set<Field> mappableFields = new HashSet<>();
		ResultSetMetaData meta = rs.getMetaData();
		
		for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
			
			String rsColName = meta.getColumnLabel(col);
			
			String mappedFieldName;
			boolean customMappingPresent = rsColName_fieldName.containsKey(rsColName);
			if (customMappingPresent) {
				mappedFieldName = rsColName_fieldName.get(rsColName);
			}
			else {
				mappedFieldName = rsColName;
			}
			
			try {
				Field mappableField = type.getDeclaredField(mappedFieldName);
				mappableFields.add(mappableField);
			} catch (NoSuchFieldException e) {
				// Not mappable. If the mapped field name was pulled from a custom mapping, we should throw an error
				// letting the user know they messed up their field name; otherwise would be hard to track down
				if (customMappingPresent) {
					throw new POJOBindException(
						"Custom-mapped field " + mappedFieldName + " not found in class " + type.getName() + " for result set column " + rsColName);
				}
			}
		}
		
		return mappableFields;
	}
	
	public static <T extends Collection<T>> Collection<T> determineCollectionImpl(Class<T> collectionType) throws InstantiationException, IllegalAccessException {
		
		if (collectionType == Collection.class || collectionType == List.class) {
			return new ArrayList<>();
		}
		
		if (collectionType == Set.class) {
			return new HashSet<>();
		}
		
		if (collectionType == Deque.class || collectionType == Queue.class) {
			return new LinkedList<>();
		}
		
		return collectionType.newInstance();
	}
	
}
