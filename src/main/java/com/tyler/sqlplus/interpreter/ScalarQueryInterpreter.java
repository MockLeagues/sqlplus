package com.tyler.sqlplus.interpreter;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Type;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.conversion.ConversionRegistry;

public class ScalarQueryInterpreter extends QueryInterpreter {

	@Override
	protected boolean canInterpret(Type type) {
		if (type instanceof Class) {
			return ConversionRegistry.containsStandardReader((Class<?>) type); // We can read a scalar if we have a standard reader registered for its type
		}
		return false;
	}

	@Override
	public Object interpret(Query query, Type type, AccessibleObject context) {
		return query.fetchScalar((Class<?>) type);
	}

}
