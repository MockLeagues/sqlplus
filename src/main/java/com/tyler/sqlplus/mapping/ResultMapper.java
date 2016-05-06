package com.tyler.sqlplus.mapping;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;
import com.tyler.sqlplus.conversion.Conversion;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.utility.ReflectionUtils;
import com.tyler.sqlplus.utility.ResultSets;

/**
 * Executes the process of mapping result set rows to POJOs. Note that this class will NEVER advance the ResultSet object it encompasses
 */
public class ResultMapper {

	// Used to track objects already created from the result set so we don't make duplicates
	private Map<Class<?>, Map<Object, MappedPOJO<?>>> class_key_instance = new HashMap<>();

	// Used to keep track of which fields are mappable for different class types so we can skip them if we already know they aren't present
	private Map<Class<?>, List<Field>> class_mappableFields = new HashMap<>();
	
	private ResultSet rs;
	private Set<String> columnNames;
	
	public ResultMapper(ResultSet rs) throws SQLException {
		this.rs = rs;
		this.columnNames = ResultSets.getColumns(rs);
	}
	
	/**
	 * Maps the current row of this mapper's result set to the given POJO class
	 */
	public <T> MappedPOJO<T> mapPOJO(Class<T> mapClass) {
		return mapPOJO(mapClass, null);
	}
	
	/**
	 * Maps the current row of this mapper's result set to the given POJO class. The parentRef class is the parent class that was being mapped before another recursive
	 * call was made to map another child object on the same row. Tracking this class allows us to avoid circular references
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public <T> MappedPOJO<T> mapPOJO(Class<T> mapClass, Class<?> parentRef) {

		if (Map.class.isAssignableFrom(mapClass)) {
			return new MappedPOJO<T>((T) ResultSets.toMap(rs), null);
		}
		
		ClassMetaData meta = ClassMetaData.getMetaData(mapClass);
		
		List<Field> mappableFieldsForType = null;
		if (class_mappableFields.containsKey(mapClass)) {
			mappableFieldsForType = class_mappableFields.get(mapClass);
		} else {
			mappableFieldsForType = findMappableFields(mapClass);
			class_mappableFields.put(mapClass, mappableFieldsForType);
		}
		
		if (mappableFieldsForType.isEmpty()) { // No work to do
			return null;
		}
		
		try {
			MappedPOJO<T> mappedPOJO = assertInstance(mapClass, meta);
			
			for (Iterator<Field> fieldIter = mappableFieldsForType.iterator(); fieldIter.hasNext();) {
				Field field = fieldIter.next();
				
				if (field.isAnnotationPresent(SingleRelation.class)) {
					if (parentRef == null || parentRef != field.getType()) { // Protect against circular reference
						MappedPOJO<?> relation = mapPOJO(field.getType(), mapClass);
						
						if (relation == null) { // Relation is not mappable (left join or no join)
							fieldIter.remove(); // Ensures we don't try to process this relation again
							continue; 
						}
						
						ReflectionUtils.set(field, mappedPOJO.pojo, relation.pojo);
					}
				}
				else if (field.isAnnotationPresent(MultiRelation.class)) {
					Class<?> relationType = ReflectionUtils.getGenericType(field);
					if (parentRef == null || parentRef != relationType) { // Protect against circular reference
						
						if (!Collection.class.isAssignableFrom(field.getType())) {
							throw new MappingException("@MultiRelation annotated field '" + field.getName() + "' must be of a collection type");
						}
						
						MappedPOJO<?> relation = mapPOJO(relationType, mapClass);
						
						if (relation == null) { // Relation is not mappable (left join or no join)
							fieldIter.remove(); // Ensures we don't try to process this relation again
							continue; 
						}
						
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
					Object value = Conversion.toJavaValue(field, rs.getObject(mappedCol));
					ReflectionUtils.set(field, mappedPOJO.pojo, value);
				}
			}
			
			return mappedPOJO;
		}
		catch (InstantiationException | IllegalAccessException e1) {
			throw new MappingException("Could not construct instance of class " + mapClass.getName() + ", check that it has a public no-args constructor");
		}
		catch (Exception e2) {
			throw new MappingException("Error mapping POJO from result set row", e2);
		}
	}
	
	/**
	 * Detects which fields are mappable for the given type from this mapper's result set
	 */
	private List<Field> findMappableFields(Class<?> type) {
		ClassMetaData meta = ClassMetaData.getMetaData(type);
		
		return
		Arrays
		.stream(type.getDeclaredFields())
		.filter(f -> {
			if (f.isAnnotationPresent(SingleRelation.class) || f.isAnnotationPresent(MultiRelation.class)) {
				return true;
			}
			try {
				rs.getObject(meta.getMappedColumnName(f));
				return true;
			} catch (SQLException e) {
				return false;
			}
		})
		.collect(Collectors.toList());
	}
	
	/**
	 * Makes a new instance of the given map class for the current row, if one does not exist within the currently processed rows of the result set, or else pulls the current one
	 */
	@SuppressWarnings("unchecked")
	private <T> MappedPOJO<T> assertInstance(Class<T> mapClass, ClassMetaData meta) throws Exception {
		
		// If the POJO does not have an ID field then we can't put it in our ID lookup table to re-retrieve it
		// later, so we have no choice but to just return a new instance now
		Field keyField = meta.getKeyField();
		if (keyField == null) {
			return new MappedPOJO<T>(mapClass.newInstance(), null);
		}
		
		String keyColumnName = meta.getMappedColumnName(keyField);
		
		// If we don't have the key column in our result set then we just return a new instance now since
		// we can't track duplicate entities by key
		if (!columnNames.contains(keyColumnName)) {
			return new MappedPOJO<T>(mapClass.newInstance(), null);
		}
		
		Object key = Conversion.toJavaValue(keyField, rs.getObject(keyColumnName));
		
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
	
}
