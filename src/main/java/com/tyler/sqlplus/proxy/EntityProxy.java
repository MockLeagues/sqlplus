package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.SessionClosedException;
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
		T proxy = (T) factory.createClass().newInstance();
		
		final Set<String> gettersLoaded = new HashSet<>();
		
		((Proxy)proxy).setHandler((self, invokedMethod, proceed, args) -> {
			
			String methodName = invokedMethod.getName();
			boolean hasGetPrefix = methodName.startsWith("get");
			boolean isMethodAnnotated = invokedMethod.isAnnotationPresent(LoadQuery.class);
			boolean alreadyLoaded = gettersLoaded.contains(methodName);
			boolean doLazyLoad = !alreadyLoaded && (hasGetPrefix || isMethodAnnotated);
			
			if (doLazyLoad) {
				
				// 2 critical pieces of data we need to find in order to lazy-load
				LoadQuery loadQueryAnnot = null;
				Field loadField = null;
			
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
							throw new AnnotationConfigurationException(
								"Inferred lazy-load field '" + loadFieldName + "' not found when executing method " + invokedMethod);
						}
					}
					else {
						throw new AnnotationConfigurationException("Could not determine field to lazy-load to");
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
					if (!session.isOpen()) {
						throw new SessionClosedException("Cannot lazy-load field " + loadField + ", session is no longer open");
					}
					String sql = loadQueryAnnot.value();
					Query query = session.createQuery(sql).bind(self);
					Object result = QueryInterpreter.interpret(query, loadField.getGenericType(), loadField);
					Fields.set(loadField, self, result);
					gettersLoaded.add(methodName);
				}
			}
			
			return proceed.invoke(self, args);
		});
		
		return proxy;
	}

}
