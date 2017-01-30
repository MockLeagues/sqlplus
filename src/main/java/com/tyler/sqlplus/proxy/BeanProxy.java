package com.tyler.sqlplus.proxy;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.SessionClosedException;
import com.tyler.sqlplus.interpreter.QueryInterpreter;
import com.tyler.sqlplus.utility.Fields;
import com.tyler.sqlplus.utility.ReflectionUtility;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Produces entity proxies to use when mapping POJOs from result sets
 */
public class BeanProxy {

	/**
	 * Caches which classes are proxy-able, i.e. have fields or methods annotated with @LoadQuery
	 */
	static final Map<Class<?>, Boolean> TYPE_PROXIABLE = new HashMap<>();

	/**
	 * Creates a proxy of the given class type which will intercept method calls in order to lazy-load related entities
	 */
	public static <T> T create(Class<T> type, Session session) {
		
		final Set<String> gettersLoaded = new HashSet<>();
		
		ProxyFactory factory = new ProxyFactory();
		factory.setSuperclass(type);

		@SuppressWarnings("unchecked")
		T proxy = (T) ReflectionUtility.newInstance(factory.createClass());

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
					Type loadType = loadField.getGenericType();
					QueryInterpreter interpreter = QueryInterpreter.forType(loadType);
					Object result = interpreter.interpret(query, loadType, loadField);
					Fields.set(loadField, self, result);
					gettersLoaded.add(methodName);
				}
			}
			
			return proceed.invoke(self, args);
		});
		
		return proxy;
	}

	/**
	 * Determines if the given class type should result in proxy objects being returned when mapping POJOs.
	 * Proxy objects are returned if there is at least 1 field or method in the class with a @LoadQuery annotation
	 */
	public static boolean isProxiable(Class<?> type) {
		return TYPE_PROXIABLE.computeIfAbsent(type, t -> {

			List<AccessibleObject> fieldsAndMethods = new ArrayList<>();
			fieldsAndMethods.addAll(Arrays.asList(type.getDeclaredFields()));
			fieldsAndMethods.addAll(Arrays.asList(type.getDeclaredMethods()));

			return fieldsAndMethods.stream()
							               .filter(o -> o.isAnnotationPresent(LoadQuery.class))
							               .findFirst()
							               .isPresent();
		});
	}

}
