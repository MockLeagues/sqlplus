package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.utility.ReflectionUtils;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

/**
 * Produces entity proxies to use when mapping POJOs from result sets
 */
public class EntityProxy {

	/**
	 * Creates a proxy of the given class type which will intercept method calls in order to lazy-load related entities
	 */
	@SuppressWarnings("unchecked")
	public static <T> T create(Class<T> type, Session session) throws InstantiationException, IllegalAccessException {
		
		ProxyFactory factory = new ProxyFactory();
		factory.setSuperclass(type);
		T proxy = (T) factory.createClass().newInstance();
		
		((Proxy)proxy).setHandler(new MethodHandler() {
			
			private Set<String> gettersAlreadyLoaded = new HashSet<>();
			
			@Override
			public Object invoke(Object self, Method superclassMethod, Method methodCalled, Object[] args) throws Throwable {
				
				String methodName = superclassMethod.getName();
				if (methodName.startsWith("get") && !gettersAlreadyLoaded.contains(methodName)) {
					
					String fieldName = ReflectionUtils.extractFieldName(methodName);
					Field loadField = type.getDeclaredField(fieldName);
					
					if (loadField.isAnnotationPresent(LoadQuery.class)) {
						Object loadedResult = LazyLoader.load(self, loadField, session);
						ReflectionUtils.set(loadField, self, loadedResult);
						gettersAlreadyLoaded.add(methodName);
					}
				}
				
				return methodCalled.invoke(self, args);
			}
		});
		
		return proxy;
	}

}
