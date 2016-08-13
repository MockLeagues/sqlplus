package com.tyler.sqlplus;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.conversion.ConversionRegistry;
import com.tyler.sqlplus.conversion.DbReader;
import com.tyler.sqlplus.exception.POJOBindException;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.exception.SqlRuntimeException;
import com.tyler.sqlplus.proxy.EntityProxy;
import com.tyler.sqlplus.utility.Fields;

/**
 * Defines the contract for a class which maps a result set row to an object of type <T>
 */
public interface ResultMapper<T> {

	/**
	 * Maps a row of a ResultSet to an object of type <T>
	 */
	public T map(ResultSet rs) throws SQLException;
	
	/**
	 * Caches which classes are proxiable, i.e. have fields or methods annotated with @LoadQuery
	 */
	static final Map<Class<?>, Boolean> TYPE_PROXIABLE = new HashMap<>();
	
	
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

	/**
	 * Creates a {@link ResultMapper} which will map ResultSet rows to POJOs.
	 * If the given class type has any fields or methods annotated with @LoadQuery (denoting a lazy-loaded collection), a proxy
	 * object will be returned;
	 */
	public static <E> ResultMapper<E> forType(Class<E> klass, ConversionRegistry conversionRegistry, Map<String, String> rsCol_fieldName, Session session, boolean underscoreCamelCaseConvert) {

		// Since we iterate over the POJO class fields when mapping them from the result set, we need to invert the given map
		// to ensure the keys are the POJO class field names, not the result set columns
		Map<String, String> fieldName_rsCol = new HashMap<>();
		rsCol_fieldName.forEach((rsCol, fieldName) -> fieldName_rsCol.put(fieldName, rsCol));

		// Determine whether this mapper should return proxies or raw class instances
		boolean proxiable = TYPE_PROXIABLE.computeIfAbsent(klass, ResultMapper::isProxiable);
		
		return new ResultMapper<E>() {

			private Set<Field> loadableFields;
			
			@Override
			public E map(ResultSet rs) throws SQLException {
				
				if (loadableFields == null) {
					loadableFields = determineLoadableFields(rs, klass, rsCol_fieldName, underscoreCamelCaseConvert);
				}
				
				E instance;
				try {
					instance = proxiable ? EntityProxy.create(klass, session) : klass.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					throw new POJOBindException(
						"Could not construct instance of class " + klass.getName() + ", verify it has a public no-args constructor");
				}
				
				for (Field loadableField : loadableFields) {
					
					@SuppressWarnings("rawtypes")
					DbReader fieldReader = conversionRegistry.getReader(loadableField.getType());;
					
					String nameOfFieldToLoad = loadableField.getName();
					String rsColumnName;
					if (fieldName_rsCol.containsKey(nameOfFieldToLoad)) {
						rsColumnName = fieldName_rsCol.get(nameOfFieldToLoad);
					}
					else if (underscoreCamelCaseConvert) {
						rsColumnName = Fields.camelCaseToUnderscore(nameOfFieldToLoad);
					}
					else {
						rsColumnName = nameOfFieldToLoad;
					}
					
					Object fieldValue = fieldReader.read(rs, rsColumnName);
					try {
						Fields.set(loadableField, instance, fieldValue);
					} catch (ReflectionException e) {
						throw new POJOBindException("Unable to set field value for field " + loadableField, e);
					}
				}
				
				return instance;
			}

		};
		
	}

	/**
	 * Determines if a given class type should result in proxy objects being returned when mapping POJOs.
	 * Proxy objects are returned if there is at least 1 field or method in the class with a @LoadQuery annotation
	 */
	static boolean isProxiable(Class<?> type) {
		
		boolean presentOnField = Arrays.stream(type.getDeclaredFields())
		                               .filter(f -> f.isAnnotationPresent(LoadQuery.class))
		                               .findFirst()
		                               .isPresent();
		if (presentOnField) {
			return true;
		}
		
		// Else see if we can find one on a method
		return Arrays.stream(type.getDeclaredMethods())
		             .filter(m -> m.isAnnotationPresent(LoadQuery.class))
		             .findFirst()
		             .isPresent();
	}
	
	static Set<Field> determineLoadableFields(ResultSet rs, Class<?> type, Map<String, String> rsColName_fieldName, boolean underscoreCamelCaseConvert) throws SQLException {
		
		Set<Field> loadableFields = new HashSet<>();
		ResultSetMetaData meta = rs.getMetaData();
		
		for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
			
			String rsColName = meta.getColumnLabel(col);
			
			String mappedFieldName;
			boolean customMappingPresent = rsColName_fieldName.containsKey(rsColName);
			if (customMappingPresent) {
				mappedFieldName = rsColName_fieldName.get(rsColName);
			}
			else if (underscoreCamelCaseConvert) {
				mappedFieldName = Fields.underscoreToCamelCase(rsColName);
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
