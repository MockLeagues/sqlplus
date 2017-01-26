package com.tyler.sqlplus.proxy;

import com.tyler.sqlplus.SqlPlus;
import com.tyler.sqlplus.annotation.Transactional;
import com.tyler.sqlplus.annotation.SqlPlusInject;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.utility.Fields;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;

/**
 * Creates proxy objects which wrap {@link Transactional} annotated methods in SqlPlus transactions.
 * Proxy classes created in this manner can contain a {@link SqlPlusInject} annotated field into which
 * the sqlplus instance will be injected and therefore queried for the current session
 */
public class TransactionAwareService {

	public static <T> T create(Class<T> serviceClass, SqlPlus sqlPlus) throws InstantiationException, IllegalAccessException {
		
		ProxyFactory factory = new ProxyFactory();
		factory.setSuperclass(serviceClass);
		factory.setFilter(method -> method.isAnnotationPresent(Transactional.class));
		
		@SuppressWarnings("unchecked")
		T serviceProxy = (T) factory.createClass().newInstance();
		
		Optional<Field> sqlPlusField = findSqlPlusInjectField(serviceClass);
		if (sqlPlusField.isPresent() && sqlPlusField.get().getType() != SqlPlus.class) {
			throw new ReflectionException(
				SqlPlusInject.class + " annotated field " + sqlPlusField.get() + " must be of type " + SqlPlus.class);
		}
		
		((Proxy)serviceProxy).setHandler((self, thisMethod, proceed, args) -> {
			Object[] result = { null };
			sqlPlus.transact(session -> {
				if (sqlPlusField.isPresent()) {
					Fields.set(sqlPlusField.get(), self, sqlPlus);
				}
				result[0] = proceed.invoke(self, args);
			});
			return result[0];
		});
		
		return serviceProxy;
	}

	private static Optional<Field> findSqlPlusInjectField(Class<?> klass) {
		Class<?> searchClass = klass;
		while (searchClass != Object.class) {
			Optional<Field> injectField = Arrays.stream(searchClass.getDeclaredFields())
			                                    .filter(field -> field.isAnnotationPresent(SqlPlusInject.class))
			                                    .findFirst();
			if (injectField.isPresent()) {
				return injectField;
			}
			searchClass = searchClass.getSuperclass();
		}
		return Optional.empty();
	}

}
