package com.tyler.sqlplus.interpreter;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.annotation.MapKey;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.QueryInterpretationException;
import com.tyler.sqlplus.utility.Fields;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class MapQueryInterpreter extends QueryInterpreter {

	@Override
	public boolean canInterpret(Type type) {
		if (type instanceof ParameterizedType) {
			ParameterizedType paramType = (ParameterizedType) type;
			if (!Map.class.isAssignableFrom((Class<?>) paramType.getRawType())) {
				return false;
			}
			Type[] generics = paramType.getActualTypeArguments();
			if ("?".equals(generics[0].toString()) || "?".equals(generics[1].toString())) {
				return false;
			}
			return true;
		}
		return false;
	}

	@Override
	public Object interpret(Query query, Type type, AccessibleObject context) {
		
		if (!context.isAnnotationPresent(MapKey.class)) {
			throw new AnnotationConfigurationException(
				context + " requires a @" + MapKey.class.getSimpleName() + " annotation in order to load entities into a map"
			);
		}
		
		ParameterizedType paramType = (ParameterizedType) type;
		Class<?> valueClass = (Class<?>) paramType.getActualTypeArguments()[1];

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
					"Null value encountered for key field '" + mapKey + "' while constructing " + valueClass + " for insertion into map. " +
					"Double check your query column names match up with the entity field names");
			}
			return key;
		};
		
		return query.streamAs(valueClass).collect(toMap(entityToKey, Function.identity()));
	}

}
