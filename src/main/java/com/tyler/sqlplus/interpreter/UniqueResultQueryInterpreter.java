package com.tyler.sqlplus.interpreter;

import com.tyler.sqlplus.Query;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

public class UniqueResultQueryInterpreter extends QueryInterpreter {

	@Override
	public boolean canInterpret(Type type) {
		return type instanceof Class &&
		       !Collection.class.isAssignableFrom((Class<?>) type) &&
		       !Map.class.isAssignableFrom((Class<?>) type);
	}

	@Override
	public Object interpret(Query query, Type type, AccessibleObject context) {
		return query.getUniqueResultAs((Class<?>) type);
	}

}
