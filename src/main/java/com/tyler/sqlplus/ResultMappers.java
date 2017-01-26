package com.tyler.sqlplus;

import java.lang.reflect.AccessibleObject;
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
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.exception.SqlRuntimeException;
import com.tyler.sqlplus.proxy.EntityProxy;
import com.tyler.sqlplus.utility.Fields;

/**
 * Utilities for creating various result mappers
 */
public final class ResultMappers {

	private static final ResultMapper<String[]> STRING_ARRAY = rs -> {
		List<String> row = new ArrayList<>();
		ResultSetMetaData meta = rs.getMetaData();
		for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
			row.add(rs.getString(col));
		}
		return row.toArray(new String[row.size()]);
	};
	
	private static final ResultMapper<Map<String, Object>> MAP = forMap(HashMap.class);

	/**
	 * Caches which classes are proxyable, i.e. have fields or methods annotated with @LoadQuery
	 */
	static final Map<Class<?>, Boolean> TYPE_PROXIABLE = new HashMap<>();
	
	private ResultMappers() {}

	/**
	 * Creates a result mapper which maps rows to string arrays
	 */
	public static ResultMapper<String[]> forStringArray() {
		return STRING_ARRAY;
	}
	
	/**
	 * Creates a result mapper which maps rows to hash maps
	 */
	public static ResultMapper<Map<String, Object>> forMap() {
		return MAP;
	}
	
	/**
	 * Creates a result mapper which maps rows to java Map objects of the given implementation
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static ResultMapper<Map<String, Object>> forMap(Class<? extends Map> impl) {
		return rs -> {
			Map<String, Object> row;
			try {
				row = impl.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new SqlRuntimeException("Could not instantiate instance of map implementation " + impl);
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
	public static <E> ResultMapper<E> forClass(Class<E> klass, ConversionRegistry conversionRegistry, Session session) {

		boolean shouldReturnProxy = TYPE_PROXIABLE.computeIfAbsent(klass, ResultMappers::isProxiable);
		
		return new ResultMapper<E>() {

			private Set<Field> loadableFields;
			
			@Override
			public E map(ResultSet rs) throws SQLException {
				
				if (loadableFields == null) {
					loadableFields = determineLoadableFields(rs, klass);
				}
				
				E instance;
				try {
					instance = shouldReturnProxy ? EntityProxy.create(klass, session) : klass.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					throw new RuntimeException("Could not construct instance of class " + klass.getName() + ", verify it has a public no-args constructor");
				}
				
				for (Field loadableField : loadableFields) {
					Object fieldValue = conversionRegistry.getReader(loadableField.getType()).read(rs, loadableField.getName());
					try {
						Fields.set(loadableField, instance, fieldValue);
					} catch (ReflectionException e) {
						throw new RuntimeException("Unable to set field value for field " + loadableField, e);
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
		
		List<AccessibleObject> fieldsAndMethods = new ArrayList<>();
		fieldsAndMethods.addAll(Arrays.asList(type.getDeclaredFields()));
		fieldsAndMethods.addAll(Arrays.asList(type.getDeclaredMethods()));
		
		return fieldsAndMethods.stream()
		                       .filter(o -> o.isAnnotationPresent(LoadQuery.class))
		                       .findFirst()
		                       .isPresent();
	}
	
	static Set<Field> determineLoadableFields(ResultSet rs, Class<?> type) throws SQLException {
		
		Set<Field> loadableFields = new HashSet<>();
		ResultSetMetaData meta = rs.getMetaData();
		
		for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
			
			String rsColName = meta.getColumnLabel(col);
			
			try {
				loadableFields.add(type.getDeclaredField(rsColName));
			} catch (NoSuchFieldException e) {
				// Field is not mappable for this result set
			}
		}
		
		return loadableFields;
	}
	
}
