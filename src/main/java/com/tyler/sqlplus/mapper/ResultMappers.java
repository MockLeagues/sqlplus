package com.tyler.sqlplus.mapper;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.conversion.ConversionRegistry;
import com.tyler.sqlplus.conversion.FieldReader;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.function.Functions;
import com.tyler.sqlplus.proxy.EntityProxy;
import com.tyler.sqlplus.utility.Fields;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
				throw new ReflectionException("Could not instantiate instance of map implementation " + impl, e);
			}
			
			ResultSetMetaData meta = rs.getMetaData();
			for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
				row.put(meta.getColumnLabel(col), rs.getObject(col));
			}
			
			return row;
		};
	};

	/**
	 * Creates a {@link ResultMapper} which will map ResultSet rows to objects of the given type.
	 * If the given type is determined to be a scalar value according to the given conversion registry, then
	 * a mapper will be returned which simply maps scalar values of the given type from the first column.
	 * If the given class type has any fields or methods annotated with @LoadQuery (denoting a lazy-loaded collection), a proxy
	 * object will be returned;
	 */
	public static <E> ResultMapper<E> forClass(Class<E> klass, ConversionRegistry converter, Session session) {

		// Scalar == value that cannot be reduced. These values will have dedicated readers. Therefore, if a reader exists for the type, it is scalar
		boolean isScalar = converter.containsReader(klass);
		if (isScalar) {
			return rs -> {
				if (rs.getMetaData().getColumnCount() > 1) {
					throw new SQLRuntimeException("Cannot map query with more than 1 column to scalar " + klass);
				}
				return converter.getReader(klass).read(rs, 1, klass);
			};
		}

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
					instance = shouldReturnProxy ? EntityProxy.create(klass, session) : newInstance(klass);
				} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
					throw new ReflectionException("Could not construct instance of " + klass, e);
				}

				loadableFields.forEach((loadableField, columnName) -> {
					Class<?> fieldType = loadableField.getType();
					FieldReader<?> reader = converter.getReader(fieldType);
					Object fieldValue = Functions.runSQL(() -> reader.read(rs, columnName, fieldType));
					Fields.set(loadableField, instance, fieldValue);
				});
				
				return instance;
			}

		};
		
	}

	/**
	 * Creates a new instance of the given class by invoking its default constructor. The constructor need not be public, but
	 * must exist
	 */
	static <T> T newInstance(Class<T> klass) throws NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
		try {
			Constructor<T> defaultConstructor = klass.getDeclaredConstructor();
			defaultConstructor.setAccessible(true);
			return defaultConstructor.newInstance();
		} catch (NoSuchMethodException e) { // Give a cleaner error message
			throw new ReflectionException(klass + " requires a no-argument constructor for instantation");
		}
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
