package com.tyler.sqlplus.utility;

import com.tyler.sqlplus.exception.ReflectionException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public final class ReflectionUtility {

	/**
	 * Creates a new instance of the given class by invoking its default constructor.
	 * The constructor does not need to be public
	 * @throws ReflectionException If no default constructor is found or there is an error while invoking it
	 */
	public static <T> T newInstance(Class<T> klass) {
		try {
			Constructor<T> defaultConstructor = klass.getDeclaredConstructor();
			defaultConstructor.setAccessible(true);
			return defaultConstructor.newInstance();
		} catch (NoSuchMethodException e) { // Give a cleaner error message
			throw new ReflectionException(klass + " requires a no-argument constructor for instantation");
		} catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
			throw new ReflectionException(e);
		}
	}

	/**
	 * Creates a new collection instance based on the given collection type by choosing sensible defaults based
	 * on the implementation. For instance, if the collection type is found to be of type {@link Set}, a
	 * {@link HashSet} will be returned
	 */
	@SuppressWarnings("rawtypes")
	public static Collection newCollection(Class<? extends Collection> collectionType) {
		if (collectionType == Collection.class || collectionType == List.class) {
			return new ArrayList<>();
		}
		else if (collectionType == Set.class) {
			return new HashSet<>();
		}
		else if (collectionType == SortedSet.class) {
			return new TreeSet<>();
		}
		else if (collectionType == Deque.class || collectionType == Queue.class) {
			return new LinkedList<>();
		}
		else {
			try {
				return collectionType.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new ReflectionException(e);
			}
		}
	}
	
	public static boolean isArray(Object obj) {
		if (obj == null) {
			return false;
		}
		return obj instanceof Object[]  ||
		       obj instanceof int[]     ||
		       obj instanceof short[]   ||
		       obj instanceof long[]    ||
		       obj instanceof float[]   ||
		       obj instanceof double[]  ||
		       obj instanceof boolean[] ||
		       obj instanceof char[]    ||
		       obj instanceof byte[];
	}

	/**
	 * Searches the given class and all superclasses for a field with the given annotation type
	 */
	public static Optional<Field> findFieldWithAnnotation(Class<? extends Annotation> annotType, Class<?> klass) {
		if (klass.isInterface()) {
			return Optional.empty();
		}
		Class<?> searchClass = klass;
		while (searchClass != Object.class) {
			Optional<Field> injectField = Arrays.stream(searchClass.getDeclaredFields())
			                                    .filter(field -> field.isAnnotationPresent(annotType))
			                                    .findFirst();
			if (injectField.isPresent()) {
				return injectField;
			}
			searchClass = searchClass.getSuperclass();
		}
		return Optional.empty();
	}
	
}
