package com.tyler.sqlplus.utility;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
	
	public static Type[] getGenericTypes(Field field) {
		try {
			ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
			return parameterizedType.getActualTypeArguments();
		}
		catch (ClassCastException cce) {
			throw new IllegalArgumentException(field + " does not contain generic type info");
		}
	}
	
	public static Type[] getGenericReturnTypes(Method method) {
		try {
			ParameterizedType parameterizedType = (ParameterizedType) method.getGenericReturnType();
			return parameterizedType.getActualTypeArguments();
		}
		catch (ClassCastException cce) {
			throw new IllegalArgumentException(method + " does not contain generic return type info");
		}
	}
	
}
