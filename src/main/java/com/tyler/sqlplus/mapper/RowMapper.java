package com.tyler.sqlplus.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Defines the contract for a class which maps a result set row to an object of type <T>
 */
@FunctionalInterface
public interface RowMapper<T> {

	/**
	 * Maps a row of a ResultSet to an object of type <T>
	 */
	T map(ResultSet rs) throws SQLException;
	
}
