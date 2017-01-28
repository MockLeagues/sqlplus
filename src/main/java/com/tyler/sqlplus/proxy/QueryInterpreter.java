package com.tyler.sqlplus.proxy;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.annotation.MapKey;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.QueryInterpretationException;
import com.tyler.sqlplus.utility.Fields;
import com.tyler.sqlplus.utility.ReflectionUtility;

public class QueryInterpreter {

	/**
	 * Interprets the given query as the given type. Class types will result in the query being returned
	 * as a single instance of that class, while collection or map types will return collections based
	 * on the generic type info available
	 */
	static Object interpret(Query query, Type type, AccessibleObject context) {
		
		if (type instanceof Class) {
			
			if (Collection.class.isAssignableFrom((Class<?>) type) || Map.class.isAssignableFrom((Class<?>) type)) {
				throw new QueryInterpretationException("Cannot interpret query '" + query + "' as " + (Class<?>) type + "; no generic info is present");
			}
			return query.getUniqueResultAs((Class<?>) type);
			
		} else if (type instanceof ParameterizedType) {
			
			ParameterizedType paramType = (ParameterizedType) type;
			Class<?> rawType = (Class<?>) paramType.getRawType();
			
			if (Collection.class.isAssignableFrom(rawType)) {
				return interpretCollection(query, paramType, context);
			}
			else if (Map.class.isAssignableFrom(rawType)) {
				return interpretMap(query, paramType, context);
			}
			else {
				return query.getUniqueResultAs(rawType);
			}
			
		} else {
			throw new QueryInterpretationException("Cannot interpret query '" + query + "' as " + type);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Collection interpretCollection(Query query, ParameterizedType paramType, AccessibleObject context) {
		
		Class<? extends Collection> collectionType = (Class<? extends Collection>) paramType.getRawType();
		Collection collectionImpl = ReflectionUtility.newCollection(collectionType);
		
		Type genericType = paramType.getActualTypeArguments()[0];
		if ("?".equals(genericType.toString())) {
			throw new QueryInterpretationException("Cannot interpret query '" + query + "' as " + paramType + "; only wildcard generic info is present");
		}
		
		return query.streamAs((Class<?>) genericType).collect(toCollection(() -> collectionImpl));
	}
	
	@SuppressWarnings("rawtypes")
	private static Map interpretMap(Query query, ParameterizedType paramType, AccessibleObject context) {
		
		if (!context.isAnnotationPresent(MapKey.class)) {
			throw new AnnotationConfigurationException("Field " + context + " requires a @MapKey annotation in order to load entities into a map");
		}
		
		// Determine value type of our map
		Type valueType = paramType.getActualTypeArguments()[1];
		if ("?".equals(valueType.toString())) {
			throw new QueryInterpretationException("Cannot interpret query '" + query + "' as " + paramType + "; only wildcard generic types are present");
		}
		Class<?> valueClass = (Class<?>) valueType;
		
		String mapKey = context.getDeclaredAnnotation(MapKey.class).value();
		Field keyField;
		try {
			keyField = valueClass.getDeclaredField(mapKey);
		} catch (NoSuchFieldException e) {
			throw new AnnotationConfigurationException("Map key field '" + mapKey + "' not found in " + valueClass);
		}

		Function<Object, Object> entityToKey = entity -> {
			Object key = Fields.get(keyField, entity);
			if (key == null) {
				throw new QueryInterpretationException(
					"Null value encountered for key field '" + mapKey + "' while constructing " + valueType + " for insertion into map. " +
					"Double check your query column names match up with the entity field names");
			}
			return key;
		};
		
		return query.streamAs(valueClass).collect(toMap(entityToKey, Function.identity()));
		
	}
	
}
