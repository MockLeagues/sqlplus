package com.tyler.sqlplus.utility;

import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.function.Functions;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

public final class ReflectionUtility {

	/**
	 * Creates a new instance of the given class by invoking its default constructor.
	 * The constructor does not need to be public
	 * @throws ReflectionException If no default constructor is found or there is an error while invoking it
	 */
	public static <T> T newInstance(Class<T> klass) {
		try {
			return withAccess(klass.getDeclaredConstructor(), constructor -> constructor.newInstance());
		} catch (NoSuchMethodException e) { // Give a cleaner error message
			throw new ReflectionException(klass + " requires a no-argument constructor for instantiation");
		} catch (Exception e) {
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

	/**
	 * Executes an action for each argument in the given parameter.
	 * <br/><br/>
	 * If the argument is an instance of {@link Iterable}, the action will be executed for each element in the iterable.
	 * <br/>
	 * If the argument is an array, the action will be executed for each element in the array
	 * <br/>
	 * Otherwise, the action will be executed for the single argument
	 */
	public static void each(Object obj, Functions.ThrowingConsumer action) throws Exception {

		if (obj == null) {
			return;
		}

		if (obj instanceof Iterable) {
			for (Object element : (Iterable) obj) {
				action.accept(element);
			}
		}
		else if (isArray(obj)) {
			int arrayLength = Array.getLength(obj);
			for (int index = 0; index < arrayLength; index++) {
				Object arrayElement = Array.get(obj, index);
				action.accept(arrayElement);
			}
		}
		else {
			action.accept(obj);
		}

	}

	/**
	 * Allows a block of code to be run in which the given accessible object will have its accessible flag set to true.
	 * When the method terminates, the accessible flag will be equal to the value it originally had at the start of
	 * this method
	 */
	public static <T extends AccessibleObject, O> O withAccess(T obj, Functions.ThrowingFunction<T, O> action) throws Exception {
		if (obj.isAccessible()) {
			return action.apply(obj);
		} else {
			obj.setAccessible(true);
			O result = action.apply(obj);
			obj.setAccessible(false);
			return result;
		}
	}

}
