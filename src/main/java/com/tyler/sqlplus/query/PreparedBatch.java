package com.tyler.sqlplus.query;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.exception.SQLSyntaxException;
import com.tyler.sqlplus.mapping.ClassMetaData;
import com.tyler.sqlplus.utility.ReflectionUtils;

public class PreparedBatch<T> {

	private final Connection conn;
	private final String stmt;
	private final List<T> entities;
	
	public PreparedBatch(Connection conn, String sql) {
		this(conn ,sql, new ArrayList<>());
	}
	
	public PreparedBatch(Connection conn, String sql, List<T> entities) {
		this.conn = conn;
		this.stmt = sql;
		this.entities = entities;
	}
	
	public PreparedBatch<T> addEntity(T entity) {
		this.entities.add(entity);
		return this;
	}
	
	public int[] executeBatch() {
		
		try {
			List<String> paramLabels = Query.parseParamLabels(stmt);
			PreparedStatement ps = conn.prepareStatement(stmt.replaceAll(Query.REGEX_PARAM.pattern(), "?"));
			
			for (Object entity : entities) {
				
				ClassMetaData meta = ClassMetaData.getMetaData(entity.getClass());
				int paramIndex = 1;
				
				for (String paramLabel : paramLabels) {
					Field fieldForParam = meta.getMappedField(paramLabel);
					if (fieldForParam == null) {
						throw new MappingException("No member exists in class " + entity.getClass().getName() + " to bind a value for parameter '" + paramLabel + "'");
					}
					Object paramValue = ReflectionUtils.get(fieldForParam, entity);
					ps.setObject(paramIndex++, paramValue);
				}
				
				ps.addBatch();
			}
			
			return ps.executeBatch();
		} catch (SQLException e) {
			throw new SQLSyntaxException(e);
		} catch (IllegalArgumentException | IllegalAccessException | SecurityException e) {
			throw new MappingException(e);
		}
	}
	
}
