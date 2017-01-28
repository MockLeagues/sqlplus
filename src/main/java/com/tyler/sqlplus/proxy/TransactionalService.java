package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
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
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.function.ReturningWork;
import com.tyler.sqlplus.utility.Fields;

import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

/**
 * Creates proxy objects which wrap {@link Transactional} annotated methods in SqlPlus transactions.
 * Proxy classes created in this manner can contain a {@link SQLPlusInject} annotated field into which
 * the sqlplus instance will be injected and therefore queried for the current session
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
		
		Optional<Field> sqlPlusField = Fields.findFieldWithAnnotation(SQLPlusInject.class, serviceClass);
		if (sqlPlusField.isPresent() && sqlPlusField.get().getType() != SQLPlus.class) {
			throw new ReflectionException("@" + SQLPlusInject.class.getSimpleName() + " annotated field " + sqlPlusField.get() + " must be of type " + SQLPlus.class);
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

	static Object invokeUpdate(Method queryMethod, Object[] invokeArgs, Session session) throws Exception {
		Query query = session.createQuery(queryMethod.getAnnotation(SQLPlusUpdate.class).value());
		bindParams(query, queryMethod.getParameters(), invokeArgs);
		query.executeUpdate();
		return null;
	}
	
	static Object invokeQuery(Method queryMethod, Object[] invokeArgs, Session session) throws Exception {
		
		Class<?> returnType = queryMethod.getReturnType();
		if (returnType == void.class) {
			throw new SQLRuntimeException("@" + SQLPlusQuery.class.getSimpleName() + " annotated method " + queryMethod + " must declare a return type");
		}
		
		Query query = session.createQuery(queryMethod.getAnnotation(SQLPlusQuery.class).value());
		bindParams(query, queryMethod.getParameters(), invokeArgs);
		return QueryInterpreter.interpret(query, queryMethod.getGenericReturnType(), queryMethod);
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
