package com.tyler.sqlplus.utility;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.tyler.sqlplus.exception.SQLRuntimeException;

/**
 * Encapsulates iteration / functional streaming over a result set
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
				throw new SQLRuntimeException(e);
			}
		}

		@Override
		public ResultSet next() {
			return rs;
		}
		
	};
	
	/**
	 * Returns a stream over the given ResultSet
	 */
	public static Stream<ResultSet> stream(ResultSet rs) throws SQLException {
		Iterator<ResultSet> rsIter = new ResultIterator(rs);
		Spliterator<ResultSet> rsSpliterator = Spliterators.spliteratorUnknownSize(rsIter, Spliterator.ORDERED);
		return StreamSupport.stream(rsSpliterator, false);
	}
	
}
