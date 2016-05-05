package com.tyler.sqlplus.query;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Tiny wrapper class over the main query class which contains a declared result type
 */
public class TypedQuery<T> {

	private final Class<T> type;
	private final Query q;
	
	public TypedQuery(Class<T> type, String sql, Connection conn) {
		this.type = type;
		this.q = new Query(sql, conn);
	}

	public List<T> fetchResults() {
		return q.fetchAs(type);
	}
	
	public T fetchSingleResult() {
		return q.findAs(type);
	}
	
	public Stream<T> stream() {
		return q.streamAs(type);
	}

}
