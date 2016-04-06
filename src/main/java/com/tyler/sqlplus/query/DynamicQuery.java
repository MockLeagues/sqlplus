package com.tyler.sqlplus.query;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is a builder-like class which allow dynamic queries to be easily be constructed
 */
public class DynamicQuery {

	private List<String> stmts = new ArrayList<>();
	private List<Object> params = new ArrayList<>();
	private Connection conn;
	
	public DynamicQuery(Connection conn) {
		this.conn = conn;
	}
	
	public DynamicQuery query(String sql, Object... params) {
		this.stmts.add(sql);
		for (Object o : params) this.params.add(o);
		return this;
	}
	
	public DynamicQuery queryIf(boolean test, String sql, Object... params) {
		return test ? query(sql, params) : this;
	}
	
	public DynamicQuery queryIfNotNull(Object param, String sql) {
		return queryIf(param != null, sql, param);
	}
	
	public Query build() {
		int p = 1;
		LinkedHashMap<String, Object> paramMap = new LinkedHashMap<>();
		for (Object o : params) {
			paramMap.put(p++ + "", o);
		}
		return  new Query(stmts.stream().collect(Collectors.joining(" ")), conn, paramMap);
	}
	
}
