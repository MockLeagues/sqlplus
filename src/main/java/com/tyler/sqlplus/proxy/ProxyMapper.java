package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.tyler.sqlplus.ResultMapper;
import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.conversion.AttributeConverter;
import com.tyler.sqlplus.conversion.ConversionPolicy;
import com.tyler.sqlplus.exception.POJOBindException;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.utility.ReflectionUtils;

/**
 * Creates {@link ResultMapper} objects which map rows of a result set to entity proxies
 */
public class ProxyMapper {

	@SuppressWarnings("unchecked")
	public static <E> ResultMapper<E> forType(Class<E> type, Session session) throws SQLException {
		return forType(type, Collections.EMPTY_MAP, session);
	}
	
	public static <E> ResultMapper<E> forType(Class<E> type, Map<String, String> rsCol_fieldName, Session session) throws SQLException {
		return forType(type, ConversionPolicy.DEFAULT, rsCol_fieldName, session);
	}
	
	public static <E> ResultMapper<E> forType(Class<E> type, ConversionPolicy conversionPolicy, Map<String, String> rsCol_fieldName, Session session) {

		// Since we iterate over the POJO class fields when mapping them from the result set, we need to invert the given map
		// to ensure the keys are the POJO class field names, not the result set columns
		Map<String, String> fieldName_rsCol = new HashMap<>();
		rsCol_fieldName.forEach((rsCol, fieldName) -> fieldName_rsCol.put(fieldName, rsCol));
		
		return new ResultMapper<E>() {

			private Set<Field> loadableFields;
			
			@Override
			public E map(ResultSet rs) throws SQLException {
				
				if (loadableFields == null) {
					loadableFields = determineLoadableFields(rs, type, rsCol_fieldName);
				}
				
				E instance;
				try {
					instance = EntityProxy.create(type, session);
				} catch (InstantiationException | IllegalAccessException e) {
					throw new POJOBindException(
						"Could not construct instance of class " + type.getName() + ", verify it has a public no-args constructor");
				}
				
				for (Field loadableField : loadableFields) {
					
					AttributeConverter<?> converterForField = conversionPolicy.findConverter(loadableField.getType());
					
					String nameOfFieldToLoad = loadableField.getName();
					String rsColumnName;
					if (fieldName_rsCol.containsKey(nameOfFieldToLoad)) {
						rsColumnName = fieldName_rsCol.get(nameOfFieldToLoad);
					}
					else {
						rsColumnName = nameOfFieldToLoad;
					}
					
					Object fieldValue = converterForField.get(rs, rsColumnName);
					try {
						ReflectionUtils.set(loadableField, instance, fieldValue);
					} catch (ReflectionException e) {
						throw new POJOBindException("Unable to set field value for field " + loadableField, e);
					}
				}
				
				return instance;
			}

		};
		
	}

	@SuppressWarnings("unchecked")
	public static Set<Field> determineLoadableFields(ResultSet rs, Class<?> type) throws SQLException {
		return determineLoadableFields(rs, type, Collections.EMPTY_MAP);
	}
	
	public static Set<Field> determineLoadableFields(ResultSet rs, Class<?> type, Map<String, String> rsColName_fieldName) throws SQLException {
		
		Set<Field> loadableFields = new HashSet<>();
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
				Field loadableField = type.getDeclaredField(mappedFieldName);
				loadableFields.add(loadableField);
			} catch (NoSuchFieldException e) {
				// Not mappable. If the mapped field name was pulled from a custom mapping, we should throw an error
				// letting the user know they messed up their field name; otherwise would be hard to track down
				if (customMappingPresent) {
					throw new POJOBindException(
						"Custom-mapped field " + mappedFieldName + " not found in class " + type.getName() + " for result set column " + rsColName);
				}
			}
		}
		
		return loadableFields;
	}
	
}
