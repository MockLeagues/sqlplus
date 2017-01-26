package com.tyler.sqlplus.conversion;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Defines logic for writing a java object to a {@link PreparedStatement}
 */
@FunctionalInterface
public interface FieldWriter<T> {

	public void write(PreparedStatement ps, int parameterIndex, T obj) throws SQLException;
	
}
