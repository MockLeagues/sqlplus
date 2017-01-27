package com.tyler.sqlplus.mapper;

import com.tyler.sqlplus.exception.SqlRuntimeException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Encapsulates iteration / function streaming over a result set
 */
public class ResultStream {

	private static class ResultIterator implements Iterator<ResultSet> {
		
		private ResultSet rs;
		
		public ResultIterator(ResultSet rs) throws SQLException {
			this.rs = rs;
		}
		
		@Override
		public boolean hasNext() {
			try {
				return rs.next();
			} catch (SQLException e) {
				throw new SqlRuntimeException(e);
			}
		}

		@Override
		public ResultSet next() {
			return rs;
		}
		
	};
	
	public static Stream<ResultSet> stream(ResultSet rs) throws SQLException {
		Iterator<ResultSet> rsIter = new ResultIterator(rs);
		Spliterator<ResultSet> rsSpliterator = Spliterators.spliteratorUnknownSize(rsIter, Spliterator.ORDERED);
		return StreamSupport.stream(rsSpliterator, false);
	}
	
}
