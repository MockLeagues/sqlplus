package com.tyler.sqlplus.conversion;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Manages conversion of java objects to (if being set on a {@link PreparedStatement}) and from (if being read from a {@link ResultSet}) database values
 */
public interface AttributeConverter<T> {

	public T get(ResultSet rs, int column) throws SQLException;

	public void set(PreparedStatement ps, int parameterIndex, T obj) throws SQLException;
	
}
