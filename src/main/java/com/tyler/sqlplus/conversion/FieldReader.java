package com.tyler.sqlplus.conversion;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Defines logic for reading a java object from a {@link ResultSet}
 */
@FunctionalInterface
public interface FieldReader<T> {

	public default T read(ResultSet rs, int colIndex) throws SQLException {
		String labelForIndex = rs.getMetaData().getColumnLabel(colIndex);
		return read(rs, labelForIndex);
	}
	
	public T read(ResultSet rs, String column) throws SQLException;
	
}
