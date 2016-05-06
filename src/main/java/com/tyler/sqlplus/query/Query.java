package com.tyler.sqlplus.query;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.tyler.sqlplus.conversion.Conversion;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.exception.NoResultsException;
import com.tyler.sqlplus.exception.NonUniqueResultException;
import com.tyler.sqlplus.exception.SQLSyntaxException;
import com.tyler.sqlplus.mapping.ClassMetaData;
import com.tyler.sqlplus.mapping.MappedPOJO;
import com.tyler.sqlplus.mapping.ResultMapper;
import com.tyler.sqlplus.utility.ReflectionUtils;
import com.tyler.sqlplus.utility.ResultSets;

public class Query {

	private static final Pattern REGEX_PARAM = Pattern.compile(":\\w+");
	
	private final String sql;
	private final Connection conn;
	private Map<String, Object> paramMap;
	private LinkedHashSet<String> paramLabels;
	
	// This is a counter which is incremented each time a raw parameter is added to the query.
	// It is then used as a key to store the parameter in the param map
	private int rawParamCounter;
	
	public Query(String sql, Connection conn) {
		
		this.sql = sql.replaceAll(REGEX_PARAM.pattern(), "?");;
		this.conn = conn;
		this.rawParamCounter = 0;
		this.paramMap = new HashMap<>();
		
		// We parse out the parameters immediately so we don't constantly have to perform a string match
		// each time a parameter is set to validate it exists
		this.paramLabels = new LinkedHashSet<>();
		Matcher paramsMatcher = REGEX_PARAM.matcher(sql);
		while (paramsMatcher.find()) {
			paramLabels.add(paramsMatcher.group().substring(1));
		}
	}
	
	/**
	 * Adds a raw ordinal parameter to this query. This method should be used to specify values for '?' params in your query
	 * 
	 * Note that if you mix calls to addParameter() and setParameter() for any given query, the results will be undefined. It is
	 * therefore highly recommended not to mix calls to these two methods within a single query
	 */
	public Query addParameter(Object param) {
		String counter = rawParamCounter++ + "";
		this.paramLabels.add(counter);
		this.paramMap.put(counter, param);
		return this;
	}
	
	public Query setParameter(String key, Object val) {
		if (!paramLabels.contains(key)) {
			throw new SQLSyntaxException("Unknown query parameter: " + key);
		}
		this.paramMap.put(key, val);
		return this;
	}
	
	public <T> Stream<T> streamAs(Class<T> klass) throws SQLException {
		ResultSet rs = prepareStatement(false).executeQuery();
		ResultMapper mapper = new ResultMapper(rs);
		return ResultSets.rowStream(rs)
		                 .map(row -> mapper.mapPOJO(klass))
		                 .distinct()
		                 .map(MappedPOJO::getPOJO);
	}
	
	/**
	 * Executes this query, mapping the results to the given POJO class. ResultSet columns will directly map
	 * to the POJO's field names unless they are annoted with an @Column annotation to specify the mapped field.
	 * 
	 * A NoResultsException is thrown if there are no results
	 */
	public <T> List<T> fetchAs(Class<T> resultClass) {
		try {
			List<T> results = streamAs(resultClass).collect(Collectors.toList());
			if (results.isEmpty()) {
				throw new NoResultsException();
			}
			return results;
		} catch (SQLException e) {
			throw new SQLSyntaxException("Error executing query", e);
		}
	}
	
	/**
	 * Returns the result of this query as a 'scalar' (single) value of the given Java class type.
	 * 
	 * This method will throw a SQLSyntaxException if the produced result set has more than 1 column
	 */
	public <T> T fetchScalar(Class<T> scalarClass) {
		try {
			ResultSet rs = prepareStatement(false).executeQuery();
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
	public Query bindParams(Object o) {
		ClassMetaData meta = ClassMetaData.getMetaData(o.getClass());
		paramLabels.forEach(label -> {
			try {
				Field mappedField = meta.getMappedField(label);
				if (mappedField == null) {
					throw new MappingException("No member exists in class " + o.getClass().getName() + " to bind a value for parameter '" + label + "'");
				}
				Object paramValue = ReflectionUtils.get(mappedField, o);
				this.paramMap.put(label, paramValue);
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
		executeUpdate(null);
	}
	
	/**
	 * Executes this query's payload as an update statement, returning the generated keys as instances of the given class
	 */
	public <T> List<T> executeUpdate(Class<T> targetKeyClass) {
		try {
			boolean returnKeys = targetKeyClass != null;
			PreparedStatement ps = prepareStatement(returnKeys);
			int affectedRows = ps.executeUpdate();
			if (returnKeys && affectedRows > 0) {
				ResultSet autoKeys = ps.getGeneratedKeys();
				return
					ResultSets
					.rowStream(autoKeys)
					.map(rs -> {
						try {
							return rs.getObject(1);
						} catch (SQLException e) {
							throw new RuntimeException(e);
						}
					})
					.map(key -> Conversion.toJavaValue(targetKeyClass, key))
					.collect(Collectors.toList());
			}
			return null;
		} catch (SQLException e) {
			throw new SQLSyntaxException(e);
		}
	}

	/**
	 * Prepares a JDBC statement object for this query's payload using the given return auto-keys flag
	 */
	public PreparedStatement prepareStatement(boolean returnAutoKeys) throws SQLException {
		
		PreparedStatement ps = returnAutoKeys ?
		                           conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) :
		                           conn.prepareStatement(sql);

		// Validate we have all parameters set
		Set<String> missingParams = new LinkedHashSet<>(paramLabels);
		missingParams.removeAll(paramMap.keySet());
		if (!missingParams.isEmpty()) {
			throw new SQLSyntaxException("Missing parameter values for the following parameters: " + missingParams);
		}
		
		int p = 1;
		for (String paramLabel : paramLabels) { // This will be correctly ordered since we use LinkedHashSet
			Object value = paramMap.get(paramLabel);
			if (value == null) {
				ps.setString(p++, null);
			}
			else if (value instanceof Enum) {
				ps.setString(p++, value.toString());
			}
			else {
				ps.setObject(p++, value);
			}
		}
		return ps;
	}

}
