package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Optional;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.Bind;
import com.tyler.sqlplus.annotation.BindParam;
import com.tyler.sqlplus.annotation.SQLPlusInject;
import com.tyler.sqlplus.annotation.SQLPlusQuery;
import com.tyler.sqlplus.annotation.SQLPlusUpdate;
import com.tyler.sqlplus.annotation.Transactional;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
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
		factory.setSuperclass(serviceClass);
		factory.setFilter(method -> method.isAnnotationPresent(Transactional.class) ||
		                            method.isAnnotationPresent(SQLPlusQuery.class) ||
		                            method.isAnnotationPresent(SQLPlusUpdate.class));
		
		@SuppressWarnings("unchecked")
		T serviceProxy = (T) factory.createClass().newInstance();
		
		Optional<Field> sqlPlusField = ReflectionUtility.findFieldWithAnnotation(SQLPlusInject.class, serviceClass);
		if (sqlPlusField.isPresent() && sqlPlusField.get().getType() != SQLPlus.class) {
			throw new AnnotationConfigurationException("@" + SQLPlusInject.class.getSimpleName() + " annotated field " + sqlPlusField.get() + " must be of type " + SQLPlus.class);
		}
		
		((Proxy) serviceProxy).setHandler((self, overriddenMethod, proceed, args) -> {
			
			if (sqlPlusField.isPresent()) {
				Fields.set(sqlPlusField.get(), self, sqlPlus);
			}
			
			ReturningWork<Session, Object> workToDoInTransaction;
			if (overriddenMethod.isAnnotationPresent(SQLPlusQuery.class)) {
				workToDoInTransaction = session -> invokeQuery(overriddenMethod, args, session);
			} else if (overriddenMethod.isAnnotationPresent(SQLPlusUpdate.class)) {
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
			throw new AnnotationConfigurationException("@" + SQLPlusQuery.class.getSimpleName() + " annotated method " + queryMethod + " must declare a return type");
		}
		
		String sql = queryMethod.getAnnotation(SQLPlusQuery.class).value();
		Query query = session.createQuery(sql);
		bindParams(query, queryMethod.getParameters(), invokeArgs);
		
		Type genericReturnType = queryMethod.getGenericReturnType();
		QueryInterpreter interpreter = QueryInterpreter.forType(genericReturnType);
		return interpreter.interpret(query, genericReturnType, queryMethod);
	}
	
	static Object invokeUpdate(Method queryMethod, Object[] invokeArgs, Session session) throws Exception {
		String sql = queryMethod.getAnnotation(SQLPlusUpdate.class).value();
		Query query = session.createQuery(sql);
		bindParams(query, queryMethod.getParameters(), invokeArgs);
		query.executeUpdate();
		return null;
	}
	
	static void bindParams(Query query, Parameter[] params, Object[] invokeArgs) throws Exception {
		
		for (int i = 0; i < params.length; i++) {
			
			Parameter param = params[i];
			Object invokeArg = invokeArgs[i];
			
			if (param.isAnnotationPresent(BindParam.class)) {
				String queryParam = param.getAnnotation(BindParam.class).value();
				query.setParameter(queryParam, invokeArg);
			} else if (param.isAnnotationPresent(Bind.class)) {
				query.bind(invokeArg);
			}
		}
		
	}
	
}
