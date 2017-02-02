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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Produces entity proxies to use when mapping POJOs from result sets
 */
public class BeanProxy {

	/**
	 * Cache structure for lazy-load info for different class types
	 */
	static final Map<Class<?>, Map<Method, LazyLoadInfo>> LAZY_LOAD_METHODS_BY_CLASS = new HashMap<>();

	/**
	 * Creates a proxy of the given class type which will intercept method calls in order to lazy-load related entities
	 */
	public static <T> T create(Class<T> type, Session session) {
		
		ProxyFactory factory = new ProxyFactory();
		factory.setSuperclass(type);
		factory.setFilter(getLazyLoadInfo(type)::containsKey);

		@SuppressWarnings("unchecked")
		T proxy = (T) ReflectionUtility.newInstance(factory.createClass());

		final Set<Method> methodsLoaded = new HashSet<>();
		((Proxy) proxy).setHandler((self, invokedMethod, proceed, args) -> {

			boolean isFirstTimeInvocation = methodsLoaded.add(invokedMethod);
			if (isFirstTimeInvocation) {

				LazyLoadInfo lazyLoadInfo = getLazyLoadInfo(type).get(invokedMethod);
				String loadSQL = lazyLoadInfo.loadSQL;
				Field loadField = lazyLoadInfo.loadField;

				if (!session.isOpen()) {
					throw new SessionClosedException("Cannot lazy-load field " + loadField + ", session is no longer open");
				}

				Query query = session.createQuery(loadSQL).bind(self);
				Type loadType = loadField.getGenericType();
				QueryInterpreter interpreter = QueryInterpreter.forType(loadType);
				Object result = interpreter.interpret(query, loadType, loadField);
				Fields.set(loadField, self, result);
			}

			return proceed.invoke(self, args);
		});
		
		return proxy;
	}

	/**
	 * Determines if the given class type should result in proxy objects being returned when mapping POJOs.
	 * Proxy objects are returned if there is at least 1 field or method in the class with a @LoadQuery annotation
	 */
	public static boolean isProxiable(Class<?> klass) {
		return !getLazyLoadInfo(klass).isEmpty();
	}

	private static Map<Method, LazyLoadInfo> getLazyLoadInfo(Class<?> klass) {
		return LAZY_LOAD_METHODS_BY_CLASS.computeIfAbsent(klass, BeanProxy::parseLazyLoadInfo);
	}

	/**
	 * Parses out a mapping of methods which should lazy load for the given class to the respective fields
	 * and load SQL for them. A method is considered to be a lazy-loading method if either of the following is
	 * true:
	 * 1) It is directly annotated with @LoadQuery and has a java-bean style 'getter' name to which a corresponding
	 * field is found (or the field name can be directly given in @LoadQuery)
	 * 2) It is a java-bean style 'getter' method whose field is annotated with @LoadQuery
	 */
	private static Map<Method, LazyLoadInfo> parseLazyLoadInfo(Class<?> klass) {

		Map<Method, LazyLoadInfo> parsedInfo = new HashMap<>();

		for (Method method : klass.getDeclaredMethods()) {

			// 2 pieces of information we need to know how to lazy load for this method
			String loadSQL = null;
			Field loadField = null;

			if (method.isAnnotationPresent(LoadQuery.class)) {
				LoadQuery loadQuery = method.getAnnotation(LoadQuery.class);
				loadSQL = loadQuery.value();
				String fieldName = loadQuery.field().isEmpty() ? Fields.extractFieldName(method.getName()) : loadQuery.field();
				try {
					loadField = klass.getDeclaredField(fieldName);
				}
				catch (NoSuchFieldException e) {
					throw new AnnotationConfigurationException("Could not find lazy-load field '" + fieldName + "' in " + klass);
				}
			}
			else if (method.getName().startsWith("get")) {

				try {
					String fieldName = Fields.extractFieldName(method.getName());
					loadField = klass.getDeclaredField(fieldName);
				}
				catch (NoSuchFieldException e) {
					continue; // Not a standard java-bean getter property. We have no way of knowing which field this is associated with, so we must skip it
				}

				if (loadField.isAnnotationPresent(LoadQuery.class)) {
					loadSQL = loadField.getAnnotation(LoadQuery.class).value();
				}
			}

			if (loadSQL != null && loadField != null) {
				parsedInfo.put(method, new LazyLoadInfo(loadField, loadSQL));
			}

		}

		return parsedInfo;
	}

	private static class LazyLoadInfo {

		private Field loadField;
		private String loadSQL;

		public LazyLoadInfo(Field loadField, String loadSQL) {
			this.loadField = loadField;
			this.loadSQL = loadSQL;
		}

	}

}

