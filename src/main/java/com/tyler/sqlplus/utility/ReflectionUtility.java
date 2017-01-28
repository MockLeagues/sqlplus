package com.tyler.sqlplus.utility;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.tyler.sqlplus.exception.ReflectionException;

public final class ReflectionUtility {

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
