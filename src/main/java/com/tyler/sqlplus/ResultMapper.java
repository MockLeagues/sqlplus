package com.tyler.sqlplus;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.tyler.sqlplus.annotation.Column;
import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;
import com.tyler.sqlplus.conversion.Conversion;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.utility.ReflectionUtils;

/**
 * Executes the process of mapping a result set row to a POJO
 */
public class ResultMapper {

	// Used to track objects already created from the result set so we don't make duplicates
	private Map<Class<?>, Map<Object, Object>> class_key_instance = new HashMap<>();
	
	/**
	 * Maps the current row of the result set to the given POJO class
	 */
	public <T> T toPOJO(ResultSet rs, Class<T> mapClass) {
		return toPOJO(rs, mapClass, null);
	}
	
	/**
	 * Maps the current row of the result set to the given POJO class. The parentRef class is the parent class that was being mapped before another recursive
	 * call was made to map another child object on the same row. Tracking this class allows us to avoid circular references
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> T toPOJO(ResultSet rs, Class<T> mapClass, Class<?> parentRef) {
		try {
			T instance = assertInstance(mapClass, rs);
			
			Map<String, Object> columnLabel_value = ResultSets.toMap(rs);
			
			for (Field field : mapClass.getDeclaredFields()) {
				
				if (field.isAnnotationPresent(SingleRelation.class)) {
					if (parentRef == null || parentRef != field.getType()) { // Protect against circular reference
						Object relation = toPOJO(rs, field.getType(), mapClass);
						ReflectionUtils.set(field, instance, relation);
					}
				}
				else if (field.isAnnotationPresent(MultiRelation.class)) {
					Class<?> relationType = ReflectionUtils.getGenericType(field);
					if (parentRef == null || parentRef != relationType) { // Protect against circular reference
						
						if (!Collection.class.isAssignableFrom(field.getType())) {
							throw new MappingException("@MultiRelation annotated field '" + field.getName() + "' must be of a collection type");
						}
						
						Object relation = toPOJO(rs, relationType, mapClass);
						Collection relatedCollection = (Collection) ReflectionUtils.get(field, instance);
						if (relatedCollection == null) {
							relatedCollection = new ArrayList<>();
							ReflectionUtils.set(field, instance, relatedCollection);
						}
						if (!relatedCollection.contains(relation)) {
							relatedCollection.add(relation);
						}
					}
				}
				else {
					String mappedCol = getMappedColName(field);
					if (!columnLabel_value.containsKey(mappedCol)) {
						throw new MappingException("Could not map pojo field '" + field.getName() + "' to any column in result set");
					}
					try {
						Object value = columnLabel_value.get(mappedCol);
						value = Conversion.toEntityValue(value, field); // Apply conversion
						ReflectionUtils.set(field, instance, value);
					}
					catch (Exception e) {
						throw new MappingException("Error setting field '" + field.getName() + "' in class " + mapClass.getName(), e);
					}
				}
			}
			
			return instance;
		}
		catch (Exception e) {
			throw new MappingException("Error mapping POJO from result set row", e);
		}
	}
	
	/**
	 * Makes a new instance of the given map class for the current row, if one does not exist within the currently processed rows of the result set, or else pulls the current one
	 */
	@SuppressWarnings("unchecked")
	private <T> T assertInstance(Class<T> mapClass, ResultSet row) {
		
		try {
			// Grab the key of this row based on the map class (remains null if we do not have a field annotated as key)
			Object key = null;
			Optional<Field> optIdField = Arrays.stream(mapClass.getDeclaredFields())
			                                   .filter(f -> f.isAnnotationPresent(Column.class) && f.getDeclaredAnnotation(Column.class).key())
			                                   .findFirst();
			
			// We only care about looking up existing entities by ID if we have an ID column for the class, otherwise we just return the new instance
			if (!optIdField.isPresent()) {
				return mapClass.newInstance();
			}
				
			Field idField = optIdField.get();
			String colName = getMappedColName(idField);
			key = Conversion.toEntityValue(row.getObject(colName), idField);
			
			// Assert we have a lookup table for the key -> instance for this type
			Map<Object, Object> key_instance = class_key_instance.get(mapClass);
			if (key_instance == null) {
				key_instance = new HashMap<>();
				class_key_instance.put(mapClass, key_instance);
			}
			
			T instance = null;
			if (key_instance.containsKey(key)) {
				instance = (T) key_instance.get(key);
			}
			else {
				instance = mapClass.newInstance();
				key_instance.put(key, instance);
			}
			return instance;
		}
		catch (Exception e) {
			throw new MappingException("Could not instantiate instance of POJO map class " + mapClass.getName(), e);
		}
	}

	private static String getMappedColName(Field field) {
		Column annot = field.getDeclaredAnnotation(Column.class);
		return annot != null && annot.name().length() > 0 ? annot.name() : field.getName();
	}
	
}
