package com.tyler.sqlplus.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.tyler.sqlplus.ResultMapper;
import com.tyler.sqlplus.ResultSets;
import com.tyler.sqlplus.conversion.Conversion;
import com.tyler.sqlplus.exception.NoResultsException;
import com.tyler.sqlplus.exception.NonUniqueResultException;
import com.tyler.sqlplus.exception.SQLSyntaxException;

public class Query {

	private static final Pattern REGEX_PARAM = Pattern.compile(":\\w+");
	
	private final String sql;
	private final Connection conn;
	private final Map<String, Object> params;
	
	public Query(String sql, Connection conn) {
		this.sql = sql;
		this.conn = conn;
		this.params = new HashMap<>();
	}

	public Query setParameter(String key, Object val) {
		if (!sql.contains(":" + key)) {
			throw new SQLSyntaxException("Unknown query parameter: " + key);
		}
		this.params.put(key, val);
		return this;
	}
	
	/**
	 * Returns a stream over the results of this query as mapped instances of the given POJO class
	 */
	public <T> Stream<T> streamAs(Class<T> mapClass) {
		try {
			ResultSet rs = execute();
			ResultMapper mapper = new ResultMapper();
			return ResultSets.rowStream(rs).map(row -> mapper.toPOJO(rs, mapClass)).distinct();
		} catch (SQLException e) {
			throw new SQLSyntaxException("Error executing query", e);
		}
	}
	
	/**
	 * Executes this query, mapping the results to the given POJO class. ResultSet columns will directly map
	 * to the POJO's field names unless they are annoted with an @Column annotation to specify the mapped field.
	 * 
	 * A NoResultsException is thrown if there are no results
	 */
	public <T> List<T> fetchAs(Class<T> resultClass) {
		List<T> uniqueResults = streamAs(resultClass).collect(Collectors.toList());
		if (uniqueResults.isEmpty()) {
			throw new NoResultsException();
		}
		return uniqueResults;
	}
	
	/**
	 * Executes this query, mapping the single result to an instance of the given POJO class. If more than 1 result is returned,
	 * a NonUniqueResultException will be thrown
	 */
	public <T> T findAs(Class<T> resultClass) {
		List<T> results = fetchAs(resultClass);
		if (results.size() != 1) {
			throw new NonUniqueResultException();
		}
		return results.get(0);
	}
	
	/**
	 * Returns the result of this query as a 'scalar' (single) value of the given Java class type.
	 * 
	 * This method will throw a SQLSyntaxException if the produced result set has more than 1 column
	 */
	public <T> T fetchScalar(Class<T> scalarClass) {
		try {
			ResultSet rs = execute();
			if (!rs.next()) {
				throw new NoResultsException();
			}
			if (rs.getMetaData().getColumnCount() > 1) {
				throw new SQLSyntaxException("Scalar query returned more than 1 column");
			}
			return Conversion.toJavaValue(scalarClass, rs.getObject(1));
		} catch (SQLException e) {
			throw new SQLSyntaxException("Error retrieving scalar value", e);
		}
	}
	
	/**
	 * Executes and retrieves the raw ResultSet object from this query's payload
	 */
	private ResultSet execute() throws SQLException {
		String nativeSql = this.sql.replaceAll(REGEX_PARAM.pattern(), "?");
		PreparedStatement ps = conn.prepareStatement(nativeSql);
		int p = 1;
		for (Object o : getOrderedParameters()) {
			ps.setObject(p++, o);
		}
		return ps.executeQuery();
	}
	
	/**
	 *  Converts the parameter map to an ordered list of objects which can be set via index on a native JDBC statement
	 */
	public List<Object> getOrderedParameters() {
		List<Object> paramList = new ArrayList<>();
		Matcher paramMatch = REGEX_PARAM.matcher(sql);
		while (paramMatch.find()) {
			String paramKey = paramMatch.group().substring(1);
			paramList.add(this.params.get(paramKey));
		}
		return paramList;
	}

}
