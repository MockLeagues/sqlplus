package com.tyler.sqlplus.mapping;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;
import com.tyler.sqlplus.conversion.Conversion;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.utility.ReflectionUtils;
import com.tyler.sqlplus.utility.ResultSets;

/**
 * Executes the process of mapping a result set row to a POJO
 */
public class ResultMapper {

	// Used to track objects already created from the result set so we don't make duplicates
	private Map<Class<?>, Map<Object, MappedPOJO<?>>> class_key_instance = new HashMap<>();

	private Set<String> resultColumnNames;
	
	/**
	 * Maps the current row of the result set to the given POJO class
	 */
	public <T> MappedPOJO<T> toPOJO(ResultSet rs, Class<T> mapClass) {
		return toPOJO(rs, mapClass, null);
	}
	
	/**
	 * Maps the current row of the result set to the given POJO class. The parentRef class is the parent class that was being mapped before another recursive
	 * call was made to map another child object on the same row. Tracking this class allows us to avoid circular references
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> MappedPOJO<T> toPOJO(ResultSet rs, Class<T> mapClass, Class<?> parentRef) {
		
		ClassMetaData meta = ClassMetaData.getMetaData(mapClass);
		
		try {
			if (resultColumnNames == null) resultColumnNames = ResultSets.getColumns(rs);
			
			MappedPOJO<T> mappedPOJO = assertInstance(mapClass, rs);
			Map<String, Object> columnLabel_value = ResultSets.toMap(rs);
			
			// Whether at least 1 field in this result set has a corresponding value in the map class
			// If this remains false after our iteration, we will return null for the POJO
			boolean hasMappedField = false;
			
			for (Field field : mapClass.getDeclaredFields()) {
				
				if (field.isAnnotationPresent(SingleRelation.class)) {
					if (parentRef == null || parentRef != field.getType()) { // Protect against circular reference
						MappedPOJO<?> relation = toPOJO(rs, field.getType(), mapClass);
						if (relation == null) continue; // Left join or no join
						ReflectionUtils.set(field, mappedPOJO.pojo, relation.pojo);
					}
				}
				else if (field.isAnnotationPresent(MultiRelation.class)) {
					Class<?> relationType = ReflectionUtils.getGenericType(field);
					if (parentRef == null || parentRef != relationType) { // Protect against circular reference
						
						if (!Collection.class.isAssignableFrom(field.getType())) {
							throw new MappingException("@MultiRelation annotated field '" + field.getName() + "' must be of a collection type");
						}
						
						MappedPOJO<?> relation = toPOJO(rs, relationType, mapClass);
						if (relation == null) continue; // Left join or no join
						
						Collection relatedCollection = (Collection) ReflectionUtils.get(field, mappedPOJO.pojo);
						if (relatedCollection == null) {
							relatedCollection = new ArrayList<>();
							ReflectionUtils.set(field, mappedPOJO.pojo, relatedCollection);
						}
						if (!relatedCollection.contains(relation.pojo)) {
							relatedCollection.add(relation.pojo);
						}
					}
				}
				else {
					String mappedCol = meta.getMappedColumnName(field);
					if (columnLabel_value.containsKey(mappedCol)) {
						hasMappedField = true;
						try {
							Object value = columnLabel_value.get(mappedCol);
							value = Conversion.toJavaValue(field, value);
							ReflectionUtils.set(field, mappedPOJO.pojo, value);
						}
						catch (Exception e) {
							throw new MappingException("Error setting field '" + field.getName() + "' in class " + mapClass.getName(), e);
						}
					}
					else { 
						// System.err.println("Could not map pojo field '" + field.getName() + "' in class " + mapClass.getName() + " to any column in " + resultColumnNames + " from result set, leaving null");
					}
				}
			}
			
			return hasMappedField ? mappedPOJO : null;
		}
		catch (Exception e) {
			throw new MappingException("Error mapping POJO from result set row", e);
		}
	}
	
	/**
	 * Makes a new instance of the given map class for the current row, if one does not exist within the currently processed rows of the result set, or else pulls the current one
	 */
	@SuppressWarnings("unchecked")
	private <T> MappedPOJO<T> assertInstance(Class<T> mapClass, ResultSet row) {
		
		try {
			ClassMetaData meta = ClassMetaData.getMetaData(mapClass);
			
			// If the POJO does not have an ID field than we can't put it in our ID lookup table to re-retrieve it later, so we just return it now
			Field keyField = meta.getKeyField();
			if (keyField == null) {
				return new MappedPOJO<T>(mapClass.newInstance(), null);
			}
			
			String keyColumnName = meta.getMappedColumnName(keyField);
			
			// If we don't have the key column in our result set then we just return the instance now since we can't set it
			if (!resultColumnNames.contains(keyColumnName)) {
				return new MappedPOJO<T>(mapClass.newInstance(), null);
			}
			
			Object key = Conversion.toJavaValue(keyField, row.getObject(keyColumnName));
			
			// Assert we have a lookup table for the key -> instance for this type
			Map<Object, MappedPOJO<?>> key_pojo = class_key_instance.get(mapClass);
			if (key_pojo == null) {
				key_pojo = new HashMap<>();
				class_key_instance.put(mapClass, key_pojo);
			}
			
			MappedPOJO<T> mappedPojo = null;
			if (key_pojo.containsKey(key)) {
				mappedPojo = (MappedPOJO<T>) key_pojo.get(key);
			}
			else {
				T pojo = mapClass.newInstance();
				mappedPojo = new MappedPOJO<T>(pojo, key);
				key_pojo.put(key, mappedPojo);
			}
			return mappedPojo;
		}
		catch (Exception e) {
			throw new MappingException("Could not instantiate instance of POJO map class " + mapClass.getName(), e);
		}
	}
	
}
