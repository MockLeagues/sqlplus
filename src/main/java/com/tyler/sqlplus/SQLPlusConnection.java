package com.tyler.sqlplus;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

import com.tyler.sqlplus.annotation.GlobalQuery;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.query.Query;
import com.tyler.sqlplus.query.TypedQuery;

/**
 * Wrapper over a standard JDBC connection
 */
public class SQLPlusConnection implements Closeable {

	private Connection conn;
	
	public SQLPlusConnection(Connection conn) {
		this.conn = conn;
	}

	public Query createQuery(String sql) {
		return new Query(sql, conn);
	}
	
	public <T> TypedQuery<T> createQuery(String sql, Class<T> type) {
		return new TypedQuery<>(type, sql, conn);
	}
	
	/**
	 * Creates a stream over every record in a database result set, mapped to the given POJO type.
	 * 
	 * The class passed to this method MUST contain a @GlobalQuery annotation to specify
	 * the query to run to retrieve all results
	 */
	public <T> Stream<T> globalStream(Class<T> mapClass) {
		if (!mapClass.isAnnotationPresent(GlobalQuery.class)) {
			throw new MappingException("Class " + mapClass.getName() + " must include a class-level @GlobalScroll annotation for global streaming");
		}
		String sql = mapClass.getDeclaredAnnotation(GlobalQuery.class).value();
		return createQuery(sql).streamAs(mapClass);
	}
	
	
	@Override
	public void close() throws IOException {
		try {
			this.conn.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
}
