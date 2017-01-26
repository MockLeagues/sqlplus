package com.tyler.sqlplus;

import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.conversion.ConversionRegistry;
import com.tyler.sqlplus.conversion.FieldReader;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.exception.SqlRuntimeException;
import com.tyler.sqlplus.proxy.EntityProxy;
import com.tyler.sqlplus.utility.Fields;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

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

			private Map<Field, String> loadableFields;
			
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
				
				loadableFields.forEach((loadableField, columnName) -> {

					FieldReader reader = conversionRegistry.getReader(loadableField.getType());

					Object fieldValue = null;
					try {
						fieldValue = reader.read(rs, columnName);
					} catch (SQLException e) {
						throw new SqlRuntimeException(e);
					}

					try {
						Fields.set(loadableField, instance, fieldValue);
					} catch (ReflectionException e) {
						throw new RuntimeException("Unable to set field value for field " + loadableField, e);
					}

				});
				
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
	
	static Map<Field, String> determineLoadableFields(ResultSet rs, Class<?> type) throws SQLException {
		
		Map<Field, String> loadableFields = new HashMap<>();
		ResultSetMetaData meta = rs.getMetaData();
		
		for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
			
			String columnLabel = meta.getColumnLabel(col);

			Field field;
			try {
				field = type.getDeclaredField(columnLabel);
			} catch (NoSuchFieldException e) {
				try {
					String convertedToCamelCase = Fields.underscoreToCamelCase(columnLabel);
					field = type.getDeclaredField(convertedToCamelCase);
				} catch (NoSuchFieldException e2) {
					continue;
				}
			}

			loadableFields.put(field, columnLabel);
		}
		
		return loadableFields;
	}
	
}
