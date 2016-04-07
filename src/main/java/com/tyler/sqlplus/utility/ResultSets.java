package com.tyler.sqlplus.utility;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utilities for working with result sets
 */
public class ResultSets {

	/**
	 * Container class for holding values of interest about a specific result set cell
	 */
	public static class Cell {
		String columnLabel, stringValue;
		Object objectValue;
		int index;
	}
	
	private static class RowIterator implements Iterator<ResultSet> {
	
		private ResultSet rs;
		
		public RowIterator(ResultSet rs) {
			this.rs = rs;
		}
		
		@Override
		public boolean hasNext() {
			try {
				return rs.next();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	
		@Override
		public ResultSet next() {
			return rs;
		}
	}
	
	private static class CellIterator implements Iterator<Cell> {
		
		private ResultSet rs;
		private ResultSetMetaData meta;
		private int currentCol = 1;
		
		public CellIterator(ResultSet rs) {
			try {
				this.rs = rs;
				this.meta = rs.getMetaData();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public boolean hasNext() {
			try {
				return currentCol <= meta.getColumnCount();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	
		@Override
		public Cell next() {
			try {
				Cell cell = new Cell();
				cell.columnLabel = meta.getColumnLabel(currentCol);
				cell.stringValue = rs.getString(currentCol);
				cell.objectValue = rs.getObject(currentCol);
				cell.index = currentCol;
				currentCol++;
				return cell;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public static Set<String> getColumns(ResultSet rs) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int count = meta.getColumnCount();
		return IntStream.rangeClosed(1, count)
		                .mapToObj(i -> {
		                	try {
		                		return meta.getColumnLabel(i);
		                	}
		                	catch (Exception e) {
		                		throw new RuntimeException(e);
		                	}
		                })
		                .collect(Collectors.toCollection(LinkedHashSet::new));
	}
	
	
	/**
	 * Returns a function which maps the row of the current result set's cursor into a map of column names to values using the HashMap implementation
	 */
	public static Map<String, Object> toMap(ResultSet rs) {
		return toMap(HashMap::new).apply(rs);
	}
	
	/**
	 * Returns a function which maps the row of the current result set's cursor into a map of column names to values using the given map implementation
	 */
	public static Function<ResultSet, Map<String, Object>> toMap(Supplier<Map<String, Object>> impl) {
		return rs -> {
			return cellStream(rs).reduce(
			                       impl.get(),
			                       (map, cell) -> { map.put(cell.columnLabel, cell.objectValue); return map; },
			                       (m1, m2) -> { m1.putAll(m2); return m2; });
		};
	};
	
	public static final String[] toArray(ResultSet rs) {
		return cellStream(rs).map(c -> c.stringValue).toArray(String[]::new);
	};
	
	/**
	 * Produces a stream over each row of the given result set. Since the result set object is always the same, it will
	 * continuously be yielded to each stream method but with a different cursor position each time
	 */
	public static Stream<ResultSet> rowStream(final ResultSet rs) {
		return asStream(() -> new RowIterator(rs));
	}

	/**
	 * Produces a stream over the given cells of the current result set row. Cells are yielded according to their ordinal positions in the row
	 */
	public static Stream<Cell> cellStream(final ResultSet rs) {
		return asStream(() -> new CellIterator(rs));
	}
	
	private static <T> Stream<T> asStream(Iterable<T> iterable) {
		return StreamSupport.stream(iterable.spliterator(), false);
	}

}
