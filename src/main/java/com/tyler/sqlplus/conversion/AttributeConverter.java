package com.tyler.sqlplus.conversion;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Manages conversion of java objects to (if being set on a {@link PreparedStatement}) and from (if being read from a {@link ResultSet}) database values
 */
public interface AttributeConverter<T> {

	public default T get(ResultSet rs, int colIndex) throws SQLException {
		String labelForIndex = rs.getMetaData().getColumnLabel(colIndex);
		return get(rs, labelForIndex);
	}
	
	public T get(ResultSet rs, String column) throws SQLException;

	public void set(PreparedStatement ps, int parameterIndex, T obj) throws SQLException;
	
	public static AttributeConverter<LocalDate> forLocalDatePattern(String pattern) {
		
		final DateTimeFormatter format = DateTimeFormatter.ofPattern(pattern);
		
		return new AttributeConverter<LocalDate>() {

			@Override
			public LocalDate get(ResultSet rs, String column) throws SQLException {
				return LocalDate.parse(rs.getString(column), format);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, LocalDate date) throws SQLException {
				String stringDate = format.format(date);
				ps.setString(parameterIndex, stringDate);
			}
			
		};
		
	}
	
}
