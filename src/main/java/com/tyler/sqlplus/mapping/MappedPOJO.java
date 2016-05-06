package com.tyler.sqlplus.mapping;

import java.util.Objects;

/**
 * Wrapper over entities mapped from database rows so we can use the entity key for the equals method, ensuring we do not map duplicates
 */
public class MappedPOJO<T> {

	public final T pojo;
	public final Object dbKey;
	
	public MappedPOJO(T pojo, Object dbKey) {
		this.pojo = pojo;
		this.dbKey = dbKey;
	}
	
	public T getPOJO() {
		return pojo;
	}
	
	@Override
	public int hashCode() {
		return dbKey == null ? super.hashCode() : dbKey.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof MappedPOJO) {
			return Objects.equals(((MappedPOJO<?>)o).dbKey, dbKey);
		}
		return false;
	}
	
}
