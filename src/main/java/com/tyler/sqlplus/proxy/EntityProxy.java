package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.exception.LazyLoadException;
import com.tyler.sqlplus.utility.Fields;

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
		
		// We intercept a method call to lazy-load only if both are true:
		// 1) We haven't loaded it yet (duh) AND
		// 2) Either the method has a @LoadQuery annotation or has a 'get' prefix
		final Set<String> gettersLoaded = new HashSet<>();
		factory.setFilter(invokedMethod -> {
			String methodName = invokedMethod.getName();
			return !gettersLoaded.contains(methodName) &&
					(methodName.startsWith("get") || invokedMethod.isAnnotationPresent(LoadQuery.class));
		});
		
		T proxy = (T) factory.createClass().newInstance();
		
		((Proxy)proxy).setHandler((self, invokedMethod, proceed, args) -> {
			
			// 2 critical pieces of data we need to find in order to lazy-load
			LoadQuery loadQueryAnnot = null;
			Field loadField = null;
			
			String methodName = invokedMethod.getName();
			boolean hasGetPrefix = methodName.startsWith("get");
			boolean isMethodAnnotated = invokedMethod.isAnnotationPresent(LoadQuery.class);
			
			if (isMethodAnnotated) {
				loadQueryAnnot = invokedMethod.getDeclaredAnnotation(LoadQuery.class);
				if (!loadQueryAnnot.field().isEmpty()) {
					loadField = type.getDeclaredField(loadQueryAnnot.field());
				}
				else if (hasGetPrefix) {
					String loadFieldName = Fields.extractFieldName(methodName);
					try {
						loadField = type.getDeclaredField(loadFieldName);
					}
					catch (NoSuchFieldException ex) {
						throw new LazyLoadException(
							"Inferred lazy-load field '" + loadFieldName + "' not found when executing method " + invokedMethod);
					}
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
				gettersLoaded.add(methodName);
			}
			
			return proceed.invoke(self, args);
		});
		
		return proxy;
	}

}
