package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.exception.LazyLoadException;
import com.tyler.sqlplus.utility.Fields;

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
			public Object invoke(Object self, Method invokedMethod, Method proceed, Object[] args) throws Throwable {
				
				String methodName = invokedMethod.getName();
				boolean hasGetPrefix = methodName.startsWith("get");
				boolean isMethodAnnotated = invokedMethod.isAnnotationPresent(LoadQuery.class);
				boolean isAlreadyLoaded = gettersAlreadyLoaded.contains(methodName);
				
				if (!isAlreadyLoaded && (hasGetPrefix || isMethodAnnotated)) {

					LoadQuery loadQueryAnnot = null;
					Field loadField = null;
					
					if (isMethodAnnotated) {
						loadQueryAnnot = invokedMethod.getDeclaredAnnotation(LoadQuery.class);
						if (!loadQueryAnnot.field().isEmpty()) {
							loadField = type.getDeclaredField(loadQueryAnnot.field());
						}
						else if (hasGetPrefix) {
							String loadFieldName = Fields.extractFieldName(methodName);
							loadField = type.getDeclaredField(loadFieldName);
						}
						else {
							throw new LazyLoadException("Could not determine field to lazy-load to");
						}
					}
					else if (hasGetPrefix) {
						String loadFieldName = Fields.extractFieldName(methodName);
						loadField = type.getDeclaredField(loadFieldName);
						if (loadField.isAnnotationPresent(LoadQuery.class)) {
							loadQueryAnnot = loadField.getDeclaredAnnotation(LoadQuery.class);
						}
					}
					
					if (loadField != null && loadQueryAnnot != null) {
						Object loadedResult = LazyLoader.load(self, loadField, loadQueryAnnot.value(), session);
						Fields.set(loadField, self, loadedResult);
						gettersAlreadyLoaded.add(methodName);
					}
				}
				
				return proceed.invoke(self, args);
			}
		});
		
		return proxy;
	}

}
