package com.tyler.sqlplus.conversion;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class SQLConverter<T> {

	public abstract Class<T> getConvertedClass();

	/**
	 * Reads a value from a {@link ResultSet}
	 */
	public T read(ResultSet rs, int colIndex, Class<?> targetType) throws SQLException {
		String labelForIndex = rs.getMetaData().getColumnLabel(colIndex);
		return read(rs, labelForIndex, targetType);
	}


	public abstract T read(ResultSet rs, String column, Class<?> targetType) throws SQLException;

	/**
	 * Writes a value to a {@Link ResultSet}
	 */
	public abstract void write(PreparedStatement ps, int parameterIndex, T obj) throws SQLException;

}
