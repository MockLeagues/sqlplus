package com.tyler.sqlplus;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tyler.sqlplus.exception.SqlRuntimeException;

/**
 * Defines the contract for a class which maps a result set row to an object of type <T>
 */
public interface ResultMapper<T> {

	public T map(ResultSet rs) throws SQLException;
	
	/**
	 * Creates a result mapper which maps rows to string arrays
	 */
	public static ResultMapper<String[]> forStringArray() {
		return rs -> {
			List<String> row = new ArrayList<>();
			ResultSetMetaData meta = rs.getMetaData();
			for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
				row.add(rs.getString(col));
			}
			return row.toArray(new String[row.size()]);
		};
	}
	
	/**
	 * Creates a result mapper which maps rows to hash maps
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Map<String, Object>> ResultMapper<T> forMap() {
		return (ResultMapper<T>) forMap(HashMap.class);
	}
	
	/**
	 * Creates a result mapper which maps rows to java Map objects of the given implementation
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Map<String, Object>> ResultMapper<T> forMap(Class<T> impl) {
		return rs -> {
			Map<String, Object> row;
			if (impl == Map.class) {
				row = new HashMap<>();
			}
			try {
				row = impl.newInstance();
			} catch (InstantiationException | IllegalAccessException e) {
				throw new SqlRuntimeException("Could not instantiate instance of map implementation " + impl.getName());
			}
			
			ResultSetMetaData meta = rs.getMetaData();
			for (int col = 1, colMax = meta.getColumnCount(); col <= colMax; col++) {
				row.put(meta.getColumnLabel(col), rs.getObject(col));
			}
			
			return (T) row;
		};
	};

}
