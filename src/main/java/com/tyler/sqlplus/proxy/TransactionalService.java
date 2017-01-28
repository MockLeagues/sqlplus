package com.tyler.sqlplus.proxy;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Optional;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.BindObject;
import com.tyler.sqlplus.annotation.BindParam;
import com.tyler.sqlplus.annotation.Database;
import com.tyler.sqlplus.annotation.DAOQuery;
import com.tyler.sqlplus.annotation.DAOUpdate;
import com.tyler.sqlplus.annotation.Transactional;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.QueryInterpretationException;
import com.tyler.sqlplus.function.ReturningWork;
import com.tyler.sqlplus.interpreter.QueryInterpreter;
import com.tyler.sqlplus.utility.Fields;
import com.tyler.sqlplus.utility.ReflectionUtility;

import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

/**
 * Creates proxy objects capable of wrapping {@link Transaction} annotated methods in SQLPlus transactions as
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
		                            method.isAnnotationPresent(DAOQuery.class) ||
		                            method.isAnnotationPresent(DAOUpdate.class));
		
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
			if (overriddenMethod.isAnnotationPresent(DAOQuery.class)) {
				workToDoInTransaction = session -> invokeQuery(overriddenMethod, args, session);
			} else if (overriddenMethod.isAnnotationPresent(DAOUpdate.class)) {
				workToDoInTransaction = session -> invokeUpdate(overriddenMethod, args, session);
			} else {
				workToDoInTransaction = session -> proceed.invoke(self, args);
			}
			
			return sqlPlus.query(workToDoInTransaction);
		});
		
		return serviceProxy;
	}

	
	static Object invokeQuery(Method queryMethod, Object[] invokeArgs, Session session) throws Exception {
		
		Class<?> returnType = queryMethod.getReturnType();
		if (returnType == void.class) {
			throw new AnnotationConfigurationException("@" + DAOQuery.class.getSimpleName() + " annotated method " + queryMethod + " must declare a return type");
		}
		
		String sql = queryMethod.getAnnotation(DAOQuery.class).value();
		Query query = session.createQuery(sql);
		bindParams(query, queryMethod.getParameters(), invokeArgs);
		
		Type genericReturnType = queryMethod.getGenericReturnType();
		QueryInterpreter interpreter = QueryInterpreter.forType(genericReturnType);
		return interpreter.interpret(query, genericReturnType, queryMethod);
	}
	
	static Object invokeUpdate(Method queryMethod, Object[] invokeArgs, Session session) throws Exception {
		
		DAOUpdate updateAnnot = queryMethod.getAnnotation(DAOUpdate.class);
		String sql = updateAnnot.value();
		Query query = session.createQuery(sql);
		bindParams(query, queryMethod.getParameters(), invokeArgs);
		
		if (updateAnnot.returnKeys()) {
			
			Class<?> keyClass;
			Type returnType = queryMethod.getGenericReturnType();
			if (returnType instanceof Class) {
				keyClass = (Class<?>) returnType;
			} else if (returnType instanceof ParameterizedType) {
				ParameterizedType paramType = (ParameterizedType) returnType;
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
			
		} else {
			query.executeUpdate();
			return null;
		}
		
	}
	
	static void bindParams(Query query, Parameter[] params, Object[] invokeArgs) throws Exception {
		
		for (int i = 0; i < params.length; i++) {
			
			Parameter param = params[i];
			Object invokeArg = invokeArgs[i];
			
			if (param.isAnnotationPresent(BindParam.class)) {
				
				String queryParam = param.getAnnotation(BindParam.class).value();
				query.setParameter(queryParam, invokeArg);
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
				else{
					query.bind(invokeArg);
				}
				
			}
		}
		
	}
	
}
