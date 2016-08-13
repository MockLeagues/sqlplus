package com.tyler.sqlplus.proxy;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.SqlPlus;
import com.tyler.sqlplus.annotation.ServiceSession;
import com.tyler.sqlplus.annotation.Transactional;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.utility.Fields;

import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

/**
 * Creates proxy objects which wrap {@link Transactional} annotated methods in SqlPlus transactions.
 * Proxy classes created in this manner can contain a {@link ServiceSession} annotated field into which the working
 * session will be injected on each service method call
 */
public class TransactionAwareService {

	public static <T> T create(Class<T> serviceClass, SqlPlus sqlPlus) throws InstantiationException, IllegalAccessException {
		
		ProxyFactory factory = new ProxyFactory();
		factory.setSuperclass(serviceClass);
		factory.setFilter(method -> method.isAnnotationPresent(Transactional.class));
		
		@SuppressWarnings("unchecked")
		T serviceProxy = (T) factory.createClass().newInstance();
		
		// Try to find the field in our service class we will inject working sessions into
		Optional<Field> sessionField = Arrays.stream(serviceClass.getDeclaredFields())
		                                     .filter(field -> field.isAnnotationPresent(ServiceSession.class))
		                                     .findFirst();
		
		if (sessionField.isPresent() && sessionField.get().getType() != Session.class) {
			throw new ReflectionException(
				"@ServiceSession annotated field " + sessionField.get() + " must be a Session type");
		}
		
		((Proxy)serviceProxy).setHandler((self, thisMethod, proceed, args) -> {
			Object[] result = { null };
			sqlPlus.transact(session -> {
				if (sessionField.isPresent()) {
					Fields.set(sessionField.get(), self, session);
				}
				result[0] = proceed.invoke(self, args);
			});
			return result[0];
		});
		
		return serviceProxy;
	}
	
}
