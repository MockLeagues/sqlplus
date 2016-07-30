package com.tyler.sqlplus.mapping;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;
import com.tyler.sqlplus.conversion.AttributeConverter;
import com.tyler.sqlplus.conversion.ConversionPolicy;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.utility.ReflectionUtils;

/**
 * Encapsulates iteration / functional streaming over a result set
 */
public class ResultStream<T> implements Iterator<MappedPOJO<T>> {

	// Used to track objects already created from the result set so we don't make duplicates
	private Map<Class<?>, Map<Object, MappedPOJO<?>>> type_key_instance = new HashMap<>();

	// Used to keep track of which fields are mappable for different class types so we can skip them if we already know they aren't present
	private Map<Class<?>, List<Field>> class_mappableFields = new HashMap<>();
	
	private ResultSet rs;
	private ResultSetMetaData meta;
	private Class<T> resultClass;
	private ConversionPolicy conversionPolicy;
	private LinkedHashMap<String, Integer> colName_colIndex;
	
	public ResultStream(ResultSet rs, Class<T> resultClass, ConversionPolicy serializer) throws SQLException {
		
		this.rs = rs;
		this.resultClass = resultClass;
		this.conversionPolicy = serializer;
		
		this.meta = rs.getMetaData();
		int columnCount = meta.getColumnCount();
		
		this.colName_colIndex = new LinkedHashMap<>();
		for (int col = 1; col <= columnCount; col++) {
			colName_colIndex.put(meta.getColumnLabel(col).toUpperCase(), col);
		}
	}
	
	/**
	 * Returns a stream over the unique mapped objects of this mapper's result set
	 */
	public Stream<T> stream() {
		Spliterator<MappedPOJO<T>> spliterator = Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED); // Convert this iterator into an ordered spliterator
		return StreamSupport.stream(spliterator, false)
		                    .distinct() // Ensures we don't get duplicate mapped entities if we have a one-to-many collection via a join
		                    .map(MappedPOJO::getPOJO);
	}
	
	@Override
	public boolean hasNext() {
		try {
			return rs.next();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public MappedPOJO<T> next() {
		if (Map.class == resultClass) {
			T row = (T) toMap();
			return new MappedPOJO<T>(row, null);
		} else {
			return mapPOJO(resultClass, null);
		}
	}
	
	/**
	 * Converts the current row of this result stream to a map
	 */
	public Map<String, String> toMap() {
		return colName_colIndex.keySet().stream().collect(Collectors.toMap(Function.identity(), col -> {
			try {
				return rs.getString(col);
			} catch (Exception e) {
				throw new SQLRuntimeException(e);
			}
		}));
	}
	
	/**
	 * Maps the current row of this mapper's result set to the given POJO class. The parentRef class is the parent class that was being mapped before another recursive
	 * call was made to map another child object on the same row. Tracking this class allows us to avoid circular references
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <E> MappedPOJO<E> mapPOJO(Class<E> mapClass, Class<?> parentRef) {
		
		ClassMetaData meta = ClassMetaData.getMetaData(mapClass);
		
		List<Field> mappableFields = getMappableFields(mapClass);
		if (mappableFields.isEmpty()) {
			return null; // No work to do
		}
		
		try {
			MappedPOJO<E> mappedPOJO = assertInstance(mapClass);
			
			for (Iterator<Field> fieldIter = mappableFields.iterator(); fieldIter.hasNext();) {
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
					String mappedCol = meta.getMappedColumnName(field).get(); // Will always be present since we are iterating over only mappable columns
					Integer resultSetIndex = colName_colIndex.get(mappedCol.toUpperCase());
					AttributeConverter converter = conversionPolicy.findConverter(field.getType());
					Object value = rs.getObject(resultSetIndex) == null ? null : converter.get(rs, resultSetIndex);
					ReflectionUtils.set(field, mappedPOJO.pojo, value);
				}
			}
			
			return mappedPOJO;
		}
		catch (InstantiationException | IllegalAccessException e1) {
			throw new MappingException("Could not construct instance of class " + mapClass.getName() + ", check that it has a public no-args constructor");
		}
		catch (Exception e) {
			throw new MappingException(e);
		}
	}
	
	/**
	 * Detects which fields are mappable for the given type from this mapper's result set
	 */
	private List<Field> getMappableFields(Class<?> type) {
		
		if (class_mappableFields.containsKey(type)) {
			return class_mappableFields.get(type);
		}
		
		ClassMetaData meta = ClassMetaData.getMetaData(type);
		List<Field> mappableFields = Arrays.stream(type.getDeclaredFields())
		                                   .filter(f -> {
		                                	   if (f.isAnnotationPresent(SingleRelation.class) || f.isAnnotationPresent(MultiRelation.class)) {
		                                		   return true;
		                                	   }
		                                	   try {
		                                		   Optional<String> colName = meta.getMappedColumnName(f);
		                                		   if (!colName.isPresent()) return false;
		                                		   rs.getObject(colName.get());
		                                		   return true;
		                                	   } catch (SQLException e) {
		                                		   return false;
		                                	   }
		                                    })
		                                    .collect(Collectors.toList());
		
		class_mappableFields.put(type, mappableFields);
		return mappableFields;
	}
	
	/**
	 * Makes a new instance of the given map class for the current row, if one does not exist within the currently processed rows
	 * of the result set, or else pulls the current one
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <E> MappedPOJO<E> assertInstance(Class<E> mapClass) throws Exception {
		
		ClassMetaData meta = ClassMetaData.getMetaData(mapClass);
		
		// If the POJO does not have an ID field then we can't put it in our ID lookup table to re-retrieve it
		// later, so we have no choice but to just return a new instance now
		Optional<Field> keyField = meta.getKeyField();
		if (!keyField.isPresent()) {
			return new MappedPOJO<>(mapClass.newInstance(), null);
		}
		
		Optional<String> keyColumnName = meta.getMappedColumnName(keyField.get());
		
		// If we don't have the key column in our result set then we just return a new instance now since
		// we can't track duplicate entities by key
		if (!keyColumnName.isPresent() || !colName_colIndex.containsKey(keyColumnName.get())) {
			return new MappedPOJO<>(mapClass.newInstance(), null);
		}
		
		Map<Object, MappedPOJO<?>> key_pojo = type_key_instance.get(mapClass);
		if (key_pojo == null) {
			key_pojo = new HashMap<>();
			type_key_instance.put(mapClass, key_pojo);
		}
		
		MappedPOJO<E> mappedPojo = null;
		AttributeConverter converter = conversionPolicy.findConverter(keyField.get().getType());
		Integer resultSetIndex = colName_colIndex.get(keyColumnName.get());
		Object key = converter.get(rs, resultSetIndex);
		if (key_pojo.containsKey(key)) {
			mappedPojo = (MappedPOJO<E>) key_pojo.get(key);
		}
		else {
			E pojo = mapClass.newInstance();
			mappedPojo = new MappedPOJO<>(pojo, key);
			key_pojo.put(key, mappedPojo);
		}
		return mappedPojo;
	}

}
