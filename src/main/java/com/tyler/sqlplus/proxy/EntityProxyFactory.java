package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.tyler.sqlplus.Query;
import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
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
					
					if (fieldForGetter.isAnnotationPresent(LoadQuery.class)) {
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Object lazyLoad(Object proxy, Field field, Session session) throws InstantiationException, IllegalAccessException {
		
		String loadSql = field.getDeclaredAnnotation(LoadQuery.class).value();
		Query loadQuery = session.createQuery(loadSql).bind(proxy);
		
		if (Collection.class.isAssignableFrom(field.getType())) {
			Class<Collection<?>> collectionType = (Class<Collection<?>>) field.getType();
			Collection collectionImpl = chooseCollectionImpl(collectionType);
			Class<?> collectionGenericType = ReflectionUtils.getGenericType(field);
			loadQuery.streamAs(collectionGenericType).forEach(collectionImpl::add);
			return collectionImpl;
		}
		else {
			return loadQuery.getUniqueResultAs(field.getType());
		}
	}

	@SuppressWarnings("unchecked")
	public static <T extends Collection<?>> T chooseCollectionImpl(Class<T> collectionType) throws InstantiationException, IllegalAccessException {
		
		if (collectionType == Collection.class || collectionType == List.class) {
			return (T) new ArrayList<>();
		}
		else if (collectionType == Set.class) {
			return (T) new HashSet<>();
		}
		else if (collectionType == SortedSet.class) {
			return (T) new TreeSet<>();
		}
		else if (collectionType == Deque.class || collectionType == Queue.class) {
			return (T) new LinkedList<>();
		}
		else {
			return collectionType.newInstance();
		}
	}
	
}
