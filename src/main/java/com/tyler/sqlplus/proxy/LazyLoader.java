package com.tyler.sqlplus.proxy;

import static java.util.stream.Collectors.*;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.annotation.MapKey;
import com.tyler.sqlplus.exception.LazyLoadException;
import com.tyler.sqlplus.exception.SessionClosedException;
import com.tyler.sqlplus.utility.Reflections;

/**
 * This class is dedicated to loading entities on-demand (lazily) for proxy objects
 */
public class LazyLoader {

	static Object load(Object proxy, Field loadField, Session session) throws InstantiationException, IllegalAccessException {
		
		if (!session.isOpen()) {
			throw new SessionClosedException("Cannot lazy-load field " + loadField + ", session is no longer open");
		}
		
		String loadSql = loadField.getDeclaredAnnotation(LoadQuery.class).value();
		Query loadQuery = session.createQuery(loadSql).bind(proxy);
		
		Object loadedResult;
		if (Collection.class.isAssignableFrom(loadField.getType())) {
			loadedResult = loadCollection(loadQuery, loadField);
		}
		else if (Map.class.isAssignableFrom(loadField.getType())) {
			loadedResult = loadMap(loadQuery, loadField);
		}
		else {
			loadedResult = loadQuery.getUniqueResultAs(loadField.getType());
		}
		
		return loadedResult;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Collection loadCollection(Query loadQuery, Field fieldToLoadTo) throws InstantiationException, IllegalAccessException {
		
		Class<? extends Collection> collectionType = (Class<? extends Collection>) fieldToLoadTo.getType();
		
		// Determine implementation to use
		Collection collectionImpl;
		if (collectionType == Collection.class || collectionType == List.class) {
			collectionImpl = new ArrayList<>();
		}
		else if (collectionType == Set.class) {
			collectionImpl = new HashSet<>();
		}
		else if (collectionType == SortedSet.class) {
			collectionImpl = new TreeSet<>();
		}
		else if (collectionType == Deque.class || collectionType == Queue.class) {
			collectionImpl = new LinkedList<>();
		}
		else {
			collectionImpl = collectionType.newInstance();
		}
		
		Type genericType = getGenericTypes(fieldToLoadTo)[0];
		if ("?".equals(genericType.toString())) {
			throw new LazyLoadException(
				"Field " + fieldToLoadTo + " contains a wildcard ('?') generic type. This is not adequate for determining the type of lazy-loaded one to many collections");
		}
		
		return loadQuery.streamAs((Class<?>) genericType).collect(toCollection(() -> collectionImpl));
	}
	
	@SuppressWarnings("rawtypes")
	private static Map loadMap(Query loadQuery, Field fieldToLoadTo) {
		
		if (!fieldToLoadTo.isAnnotationPresent(MapKey.class)) {
			throw new LazyLoadException("Field " + fieldToLoadTo + " requires a @MapKey annotation in order to load entities into a map");
		}
		
		// Determine value type of our map
		Type valueType = getGenericTypes(fieldToLoadTo)[1]; // Since we verify we are of a map type, we can be sure we have 2 generic elements
		if ("?".equals(valueType.toString())) {
			throw new LazyLoadException(
				"Field " + fieldToLoadTo + " contains a wildcard ('?') generic type for its map value. This is not adequate for determining the type of lazy-loaded one to many collections");
		}
		Class<?> valueClass = (Class<?>) valueType;
		
		String mapKey = fieldToLoadTo.getDeclaredAnnotation(MapKey.class).value();
		Field keyField;
		try {
			keyField = valueClass.getDeclaredField(mapKey);
		} catch (NoSuchFieldException | SecurityException e) {
			throw new LazyLoadException("Map key field '" + mapKey + "' not found in class " + valueClass.getName());
		}

		return loadQuery.streamAs(valueClass).collect(toMap(
			entity -> {
				Object key = Reflections.get(keyField, entity);
				if (key == null) {
					throw new LazyLoadException(
						"Null value encountered for key field '" + mapKey + "' while constructing " + valueType + " for insertion into map. " +
						"Double check your query column names match up with the entity field names");
				}
				return key;
			},
			Function.identity()
		));
	}
	
	private static Type[] getGenericTypes(Field field) {
		try {
			ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
			return parameterizedType.getActualTypeArguments();
		}
		catch (ClassCastException cce) {
			throw new LazyLoadException(
				"Field " + field + " does not contain generic type info. This is required for determining the types of lazy-loaded one to many relations");
		}
	}
	
}
