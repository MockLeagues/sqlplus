package com.tyler.sqlplus.interpreter;

import static java.util.stream.Collectors.toCollection;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.utility.ReflectionUtility;

public class CollectionQueryInterpreter extends QueryInterpreter {

	@Override
	public boolean canInterpret(Type type) {
		if (type instanceof ParameterizedType) {
			ParameterizedType paramType = (ParameterizedType) type;
			if (!Collection.class.isAssignableFrom((Class<?>) paramType.getRawType())) {
				return false;
			}
			Type[] generics = paramType.getActualTypeArguments();
			if ("?".equals(generics[0].toString())) {
				return false;
			}
			return true;
		}
		return false;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Object interpret(Query query, Type type, AccessibleObject context) {
		ParameterizedType paramType = (ParameterizedType) type;
		Class<? extends Collection> collectionType = (Class<? extends Collection>) paramType.getRawType();
		Collection collectionImpl = ReflectionUtility.newCollection(collectionType);
		Class<?> genericType = (Class<?>) paramType.getActualTypeArguments()[0];
		return query.streamAs((Class<?>) genericType).collect(toCollection(() -> collectionImpl));
	}

}
