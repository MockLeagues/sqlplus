package com.tyler.sqlplus.interpreter;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.exception.QueryInterpretationException;

/**
 * Defines the contract for a class which is able of interpreting a query as a given java type, reflectively.
 * This class is used when interpreting query results for lazy-loaded fields as well as @SQLPlusQuery method
 * return types
 */
public abstract class QueryInterpreter {

	private static final Collection<QueryInterpreter> REGISTERED_INTERPRETERS = new ArrayList<>();
	static {
		register(new DefaultQueryInterpreter());
		register(new MapQueryInterpreter());
		register(new CollectionQueryInterpreter());
	}
	
	private static final Map<Type, QueryInterpreter> INTERPRETER_INDEX = new HashMap<>();
	
	public static void register(QueryInterpreter interpreter) {
		REGISTERED_INTERPRETERS.add(interpreter);
	}
	
	public static QueryInterpreter forType(Type type) {
		if (INTERPRETER_INDEX.containsKey(type)) {
			return INTERPRETER_INDEX.get(type);
		}
		Optional<QueryInterpreter> foundInterpreter = REGISTERED_INTERPRETERS.stream()
		                                                                     .filter(i -> i.canInterpret(type))
		                                                                     .findFirst();
		if (foundInterpreter.isPresent()) {
			INTERPRETER_INDEX.put(type, foundInterpreter.get());
			return foundInterpreter.get();
		}
		throw new QueryInterpretationException("No valid query interpreters found for " + type + ". Make sure generic type info is present");
	}
	
	protected abstract boolean canInterpret(Type type);
	
	/**
	 * Interprets the given query as the given type
	 * @param query Query to interpret results for
	 * @param type Java type to interpret as
	 * @param context Field or method which can provide additional annotation information related to the query being interpreted
	 * @return The interpreted result
	 */
	public abstract Object interpret(Query query, Type type, AccessibleObject context);

}
