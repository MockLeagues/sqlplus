package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.SqlPlusLoad;
import com.tyler.sqlplus.exception.SessionClosedException;
import com.tyler.sqlplus.utility.ReflectionUtils;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

/**
 * Produces entity proxies to use when mapping POJOs from result sets
 */
public class EntityProxyFactory {

	/**
	 * Creates a proxy of the given class type which will intercept method calls in order to lazy-load related entities
	 */
	@SuppressWarnings("unchecked")
	public static <T> T create(Class<T> type, Session session) throws InstantiationException, IllegalAccessException {
		
		ProxyFactory factory = new ProxyFactory();
		factory.setSuperclass(type);
		T proxy = (T) factory.createClass().newInstance();
		
		((Proxy)proxy).setHandler(new MethodHandler() {
			
			// Tracks which getter methods have already been lazy loaded for this instance
			private Set<String> gettersAlreadyLoaded = new HashSet<>();
			
			@Override
			public Object invoke(Object self, Method superclassMethod, Method methodCalled, Object[] args) throws Throwable {
				
				String methodName = superclassMethod.getName();
				if (methodName.startsWith("get") && !gettersAlreadyLoaded.contains(methodName)) {
					
					String fieldName = ReflectionUtils.extractFieldName(methodName);
					Field fieldForGetter = type.getDeclaredField(fieldName);
					
					if (fieldForGetter.isAnnotationPresent(SqlPlusLoad.class)) {
						if (!session.isOpen()) {
							throw new SessionClosedException("Cannot lazy-load field " + fieldForGetter + ", session is no longer open");
						}
						Object loadedValue = lazyLoad(self, fieldForGetter, session);
						ReflectionUtils.set(fieldForGetter, self, loadedValue);
					}
				}
				
				return methodCalled.invoke(self, args);
			}
		});
		
		return proxy;
	}

	// TODO: handle loading collections, not just raw types
	private static Object lazyLoad(Object proxy, Field field, Session session) {
		String loadSql = field.getDeclaredAnnotation(SqlPlusLoad.class).value();
		return session.createQuery(loadSql)
		              .bind(proxy)
		              .getUniqueResultAs(field.getType());
	}
	
}
