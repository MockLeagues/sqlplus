package com.tyler.sqlplus.proxy;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.*;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.QueryInterpretationException;
import com.tyler.sqlplus.function.ReturningWork;
import com.tyler.sqlplus.interpreter.QueryInterpreter;
import com.tyler.sqlplus.utility.Fields;
import com.tyler.sqlplus.utility.ReflectionUtility;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import java.lang.reflect.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * Creates proxy objects capable of wrapping {@link Transactional} annotated methods in SQLPlus transactions as
 * well as synthesizing query and update methods
 */
public class TransactionalService {

	public static <T> T create(Class<T> serviceClass, SQLPlus sqlPlus) throws InstantiationException, IllegalAccessException {
		
		ProxyFactory factory = new ProxyFactory();
		if (serviceClass.isInterface()) {
			factory.setInterfaces(new Class[]{ serviceClass });
		} else {
			factory.setSuperclass(serviceClass);
		}
		factory.setFilter(method -> method.isAnnotationPresent(Transactional.class) ||
		                            method.isAnnotationPresent(SQLQuery.class) ||
		                            method.isAnnotationPresent(SQLUpdate.class));
		@SuppressWarnings("unchecked")
		T serviceProxy = (T) factory.createClass().newInstance();
		
		Optional<Field> sqlPlusField = ReflectionUtility.findFieldWithAnnotation(Database.class, serviceClass);
		if (sqlPlusField.isPresent()) {
			if (sqlPlusField.get().getType() != SQLPlus.class) {
				throw new AnnotationConfigurationException("@" + Database.class.getSimpleName() + " annotated field " + sqlPlusField.get() + " must be of type " + SQLPlus.class);
			}
			Fields.set(sqlPlusField.get(), serviceProxy, sqlPlus);
		}
		
		((Proxy) serviceProxy).setHandler((self, overriddenMethod, proceed, args) -> {
			
			ReturningWork<Session, Object> workToDoInTransaction;
			int isolation;

			if (overriddenMethod.isAnnotationPresent(SQLQuery.class)) {
				isolation = overriddenMethod.getAnnotation(SQLQuery.class).isolation();
				workToDoInTransaction = session -> invokeQuery(overriddenMethod, args, session);
			} else if (overriddenMethod.isAnnotationPresent(SQLUpdate.class)) {
				isolation = overriddenMethod.getAnnotation(SQLUpdate.class).isolation();
				workToDoInTransaction = session -> invokeUpdate(overriddenMethod, args, session);
			} else {
				isolation = overriddenMethod.getAnnotation(Transactional.class).isolation();
				workToDoInTransaction = session -> proceed.invoke(self, args);
			}
			
			return sqlPlus.transactAndReturn(isolation, workToDoInTransaction);
		});
		
		return serviceProxy;
	}

	
	static Object invokeQuery(Method queryMethod, Object[] invokeArgs, Session session) throws Exception {
		
		if (queryMethod.getReturnType() == void.class) {
			throw new AnnotationConfigurationException("@" + SQLQuery.class.getSimpleName() + " annotated method " + queryMethod + " must declare a return type");
		}
		
		String sql = queryMethod.getAnnotation(SQLQuery.class).value();
		Query query = session.createQuery(sql);
		bindParams(query, queryMethod.getParameters(), invokeArgs);
		
		Type genericReturnType = queryMethod.getGenericReturnType();
		QueryInterpreter interpreter = QueryInterpreter.forType(genericReturnType);
		return interpreter.interpret(query, genericReturnType, queryMethod);
	}
	
	static Object invokeUpdate(Method queryMethod, Object[] invokeArgs, Session session) throws Exception {
		
		SQLUpdate updateAnnot = queryMethod.getAnnotation(SQLUpdate.class);
		String sql = updateAnnot.value();
		Query query = session.createQuery(sql);
		bindParams(query, queryMethod.getParameters(), invokeArgs);

		switch (updateAnnot.returnInfo()) {

			case GENERATED_KEYS:

				Class<?> keyClass;
				Type genericReturnType = queryMethod.getGenericReturnType();
				if (genericReturnType instanceof Class) {
					keyClass = (Class<?>) genericReturnType;
				} else if (genericReturnType instanceof ParameterizedType) {
					ParameterizedType paramType = (ParameterizedType) genericReturnType;
					keyClass = (Class<?>) paramType.getActualTypeArguments()[0];
				} else {
					throw new QueryInterpretationException("Cannot determine key return type for " + queryMethod);
				}

				@SuppressWarnings("rawtypes")
				Collection keys = query.executeUpdate(keyClass);

				if (Collection.class.isAssignableFrom(queryMethod.getReturnType())) {
					return keys;
				} else {
					return keys.isEmpty() ? null : keys.iterator().next();
				}

			case AFFECTED_ROWS:

				int[] affectedRows = query.executeUpdate();
				Class<?> returnType = queryMethod.getReturnType();
				if (int.class == returnType || Integer.class == returnType) {
					return Arrays.stream(affectedRows).sum();
				} else if (int[].class.isAssignableFrom(returnType)) {
					return affectedRows;
				} else {
					throw new QueryInterpretationException("Cannot interpret update counts as " + returnType + ", must be either an integer or integer array");
				}

			case NONE:
			default:
				return query.executeUpdate();
		}
		
	}
	
	static void bindParams(Query query, Parameter[] params, Object[] invokeArgs) throws Exception {
		
		for (int i = 0; i < params.length; i++) {
			Parameter param = params[i];
			Object invokeArg = invokeArgs[i];

			if (param.isAnnotationPresent(BindParam.class)) {
				String paramLabel = param.getAnnotation(BindParam.class).value();
				query.setParameter(paramLabel, invokeArg);
			}
			else if (param.isAnnotationPresent(BindObject.class)) {
				
				if (invokeArg instanceof Iterable) {
					for (Object element : (Iterable<?>) invokeArg) {
						query.bind(element);
					}
				}
				else if (ReflectionUtility.isArray(invokeArg)) {
					int length = Array.getLength(invokeArg);
					for (int j = 0; j < length; j++) {
						query.bind(Array.get(invokeArg, j));
					}
				}
				else {
					query.bind(invokeArg);
				}
			}
		}
	}
	
}
