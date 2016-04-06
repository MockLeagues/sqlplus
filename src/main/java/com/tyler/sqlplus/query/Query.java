package com.tyler.sqlplus.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.tyler.sqlplus.MappedPOJO;
import com.tyler.sqlplus.ResultMapper;
import com.tyler.sqlplus.ResultSets;
import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;
import com.tyler.sqlplus.conversion.Conversion;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.exception.NoResultsException;
import com.tyler.sqlplus.exception.NonUniqueResultException;
import com.tyler.sqlplus.exception.SQLSyntaxException;
import com.tyler.sqlplus.utility.ReflectionUtils;

public class Query {

	private static final Pattern REGEX_PARAM = Pattern.compile(":\\w+");
	
	private final String sql;
	private final Connection conn;
	private LinkedHashMap<String, Object> params;
	
	public Query(String sql, Connection conn) {
		this.sql = sql.replaceAll(REGEX_PARAM.pattern(), "?");;
		this.conn = conn;
		
		// We parse out the parameters immediately so we don't constantly have to perform a string match
		// each time a parameter is set to validate it exists
		this.params = new LinkedHashMap<>();
		Matcher paramsMatcher = REGEX_PARAM.matcher(sql);
		while (paramsMatcher.find()) {
			params.put(paramsMatcher.group().substring(1), null);
		}
	}

	// Package-private constructor for the dynamic query class which sets parameters directly
	Query(String sql, Connection conn, LinkedHashMap<String, Object> params) {
		this.sql = sql;
		this.conn = conn;
		this.params = params;
	}
	
	public Query setParameter(String key, Object val) {
		if (!params.containsKey(key)) {
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
			ResultSet rs = prepareStatement().executeQuery();
			ResultMapper mapper = new ResultMapper();
			return ResultSets.rowStream(rs).map(row -> mapper.toPOJO(rs, mapClass)).distinct().map(MappedPOJO::getPOJO);
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
	 * Sets parameter values in this query using the given POJO class
	 */
	public Query bindParams(Object o, boolean bindNull) {
		Arrays
		.stream(o.getClass().getDeclaredFields())
		.filter(f -> !f.isAnnotationPresent(SingleRelation.class) && !f.isAnnotationPresent(MultiRelation.class)) // We don't bind relations
		.forEach(f -> { 
			try {
				String mappedCol = ResultMapper.getMappedColName(f);
				Object paramValue = ReflectionUtils.get(f, o);
				if ((paramValue != null || bindNull) && params.containsKey(mappedCol)) {
					this.params.put(mappedCol, paramValue);
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				throw new MappingException(e);
			}
		});
		return this;
	}
	
	/**
	 * Execute this query's payload as an update statement
	 */
	public void executeUpdate() {
		try {
			prepareStatement().executeUpdate();
		} catch (SQLException e) {
			throw new SQLSyntaxException(e);
		}
	}
	
	/**
	 * Returns the result of this query as a 'scalar' (single) value of the given Java class type.
	 * 
	 * This method will throw a SQLSyntaxException if the produced result set has more than 1 column
	 */
	public <T> T fetchScalar(Class<T> scalarClass) {
		try {
			ResultSet rs = prepareStatement().executeQuery();
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
	public PreparedStatement prepareStatement() throws SQLException {
		PreparedStatement ps = conn.prepareStatement(sql);
		int p = 1;
		for (Object value : params.values()) { // This will be correctly ordered since we use LinkedHashMap
			if (value instanceof Enum) {
				value = value.toString();
			}
			ps.setObject(p++, value);
		}
		return ps;
	}

}
