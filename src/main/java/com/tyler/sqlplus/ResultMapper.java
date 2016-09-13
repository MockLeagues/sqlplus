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
	public static ResultMapper<Map<String, Object>> forMap() {
		return forMap(HashMap.class);
	}
	
	/**
	 * Creates a result mapper which maps rows to java Map objects of the given implementation
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static ResultMapper<Map<String, Object>> forMap(Class<? extends Map> impl) {
		return rs -> {
			Map<String, Object> row;
			if (impl == Map.class) {
				row = new HashMap<>();
			}
			else {
				try {
					row = impl.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					throw new SqlRuntimeException("Could not instantiate instance of map implementation " + impl.getName());
				}
			}
			
			ResultSetMetaData meta = rs.getMetaData();
			for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
				row.put(meta.getColumnLabel(col), rs.getObject(col));
			}
			
			return row;
		};
	};

	/**
	 * Creates a {@link ResultMapper} which will map ResultSet rows to POJOs.
	 * If the given class type has any fields or methods annotated with @LoadQuery (denoting a lazy-loaded collection), a proxy
	 * object will be returned;
	 */
	public static <E> ResultMapper<E> forType(Class<E> klass, ConversionRegistry conversionRegistry, Session session, boolean underscoreCamelCaseConvert) {

		// Determine whether this mapper should return proxies or raw class instances
		boolean proxiable = TYPE_PROXIABLE.computeIfAbsent(klass, ResultMapper::isProxiable);
		
		return new ResultMapper<E>() {

			private Set<Field> loadableFields;
			
			@Override
			public E map(ResultSet rs) throws SQLException {
				
				if (loadableFields == null) {
					loadableFields = determineLoadableFields(rs, klass, underscoreCamelCaseConvert);
				}
				
				E instance;
				try {
					instance = proxiable ? EntityProxy.create(klass, session) : klass.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					throw new POJOBindException("Could not construct instance of class " + klass.getName() + ", verify it has a public no-args constructor");
				}
				
				for (Field loadableField : loadableFields) {
					
					String nameOfFieldToLoad = loadableField.getName();
					String rsColumnName;
					if (underscoreCamelCaseConvert) {
						rsColumnName = Fields.camelCaseToUnderscore(nameOfFieldToLoad);
					}
					else {
						rsColumnName = nameOfFieldToLoad;
					}
					
					Object fieldValue = conversionRegistry.getReader(loadableField.getType()).read(rs, rsColumnName);
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
	
	static Set<Field> determineLoadableFields(ResultSet rs, Class<?> type, boolean underscoreCamelCaseConvert) throws SQLException {
		
		Set<Field> loadableFields = new HashSet<>();
		ResultSetMetaData meta = rs.getMetaData();
		
		for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
			
			String rsColName = meta.getColumnLabel(col);
			
			String mappedFieldName;
			if (underscoreCamelCaseConvert) {
				mappedFieldName = Fields.underscoreToCamelCase(rsColName);
			}
			else {
				mappedFieldName = rsColName;
			}
			
			try {
				loadableFields.add(type.getDeclaredField(mappedFieldName));
			} catch (NoSuchFieldException e) {
				// Field is not mappable for this result set
			}
		}
		
		return loadableFields;
	}
	
}
