package com.tyler.sqlplus.query;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.exception.NoResultsException;
import com.tyler.sqlplus.exception.NonUniqueResultException;
import com.tyler.sqlplus.exception.SQLSyntaxException;
import com.tyler.sqlplus.mapping.ClassMetaData;
import com.tyler.sqlplus.mapping.ResultStream;
import com.tyler.sqlplus.serialization.Converter;
import com.tyler.sqlplus.utility.ReflectionUtils;

public class Query {

	private static final String REGEX_PARAM = ":\\w+";
	
	private PreparedStatement ps;
	private Set<String> paramLabels;
	private Map<String, Object> manualParamBatch;
	private List<Map<String, Object>> paramBatches;
	private Converter serializer;
	
	public Query(String sql, Connection conn) {
		
		try {
			
			this.ps = conn.prepareStatement(sql.replaceAll(REGEX_PARAM, "?"), Statement.RETURN_GENERATED_KEYS);
			this.paramBatches = new ArrayList<>();
			this.manualParamBatch = new HashMap<>();
			this.paramLabels = new LinkedHashSet<>();
			this.serializer = new Converter();
			
			// We parse out the parameters immediately so we don't constantly have to perform a string match
			// each time a parameter is set to validate it exists
			Matcher paramsMatcher = Pattern.compile(REGEX_PARAM).matcher(sql);
			while (paramsMatcher.find()) {
				this.paramLabels.add(paramsMatcher.group().substring(1));
			}
			
		} catch (SQLException e) {
			throw new SQLSyntaxException(e);
		}
	}
	
	public <T> Query setConversion(Class<T> type, Function<T, String> serializer, Function<String, T> deserializer) {
		this.serializer.setConversion(type, serializer, deserializer);
		return this;
	}
	
	/**
	 * Adds a raw ordinal parameter to the current manual parameter batch for this query. This method should be used to specify values for '?' params in your query
	 * 
	 * Note that if you mix calls to addParameter() and setParameter() for any given query, the results will be undefined. It is
	 * therefore highly recommended not to mix calls to these two methods within a single query
	 */
	public Query addParameter(Object param) {
		String counter = (manualParamBatch.size() + 1) + "";
		this.paramLabels.add(counter);
		manualParamBatch.put(counter, param);
		return this;
	}
	
	public Query setParameter(String key, Object val) {
		if (!paramLabels.contains(key)) {
			throw new SQLSyntaxException("Unknown query parameter: " + key);
		}
		manualParamBatch.put(key, val);
		return this;
	}
	
	public <T> Stream<T> streamAs(Class<T> klass) {
		try {
			applyBatches();
			ResultSet rs = ps.executeQuery();
			return new ResultStream<T>(rs, klass, serializer).stream();
		} catch (SQLException e) {
			throw new SQLSyntaxException(e);
		}
	}
	
	/**
	 * Executes this query, mapping the results to the given POJO class. ResultSet columns will directly map
	 * to the POJO's field names unless they are annoted with an @Column annotation to specify the mapped field.
	 * 
	 * A NoResultsException is thrown if there are no results
	 */
	public <T> List<T> fetchAs(Class<T> resultClass) {
		List<T> results = streamAs(resultClass).collect(Collectors.toList());
		if (results.isEmpty()) {
			throw new NoResultsException();
		}
		return results;
	}
	
	/**
	 * Returns the result of this query as a 'scalar' (single) value of the given Java class type.
	 * 
	 * This method will throw a SQLSyntaxException if the produced result set has more than 1 column
	 */
	public <T> T fetchScalar(Class<T> scalarClass) {
		try {
			applyBatches();
			ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				throw new NoResultsException();
			}
			if (rs.getMetaData().getColumnCount() > 1) {
				throw new SQLSyntaxException("Scalar query returned more than 1 column");
			}
			return serializer.deserialize(scalarClass, rs.getString(1));
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
			applyBatches();
			int[] affectedRows = ps.executeBatch();
			if (returnKeys && affectedRows[0] > 0) {
				ResultSet rsKeys = ps.getGeneratedKeys();
				List<T> keys = new ArrayList<>();
				while (rsKeys.next()) {
					String keyResult = rsKeys.getString(1);
					keys.add(serializer.deserialize(targetKeyClass, keyResult));
				}
				return keys;
			}
			return null;
		} catch (SQLException e) {
			throw new SQLSyntaxException(e);
		}
	}

	/**
	 * Finish the current running manual parameter batch
	 */
	public Query addBatch() {
		this.paramBatches.add(manualParamBatch);
		this.manualParamBatch.clear();
		return this;
	}
	
	/**
	 * Adds a batch statement using the parameters in the given POJO class.
	 */
	public Query addBatch(Object o) {
		
		Map<String, Object> newBatch = new HashMap<>();
		ClassMetaData meta = ClassMetaData.getMetaData(o.getClass());
		
		paramLabels.forEach(label -> {
			try {
				Field mappedField = meta.getMappedField(label);
				if (mappedField == null) {
					throw new MappingException("No member exists in class " + o.getClass().getName() + " to bind a value for parameter '" + label + "'");
				}
				
				Object paramValue = ReflectionUtils.get(mappedField, o);
				newBatch.put(label, paramValue);
			}
			catch (IllegalArgumentException | IllegalAccessException e) {
				throw new MappingException(e);
			}
		});
	
		paramBatches.add(newBatch);
		return this;
	}
	
	/**
	 * Applies all parameter batches stored in this query to its PreparedStatement object
	 */
	private void applyBatches() {
		
		if (!manualParamBatch.isEmpty()) {
			paramBatches.add(manualParamBatch);
		}
		
		if (paramBatches.isEmpty() && !paramLabels.isEmpty()) {
			throw new RuntimeException("No parameters set");
		}
		
		for (Map<String, Object> paramBatch : this.paramBatches) {
			try {
				
				Set<String> missingParams = new LinkedHashSet<>(paramLabels);
				missingParams.removeAll(paramBatch.keySet());
				if (!missingParams.isEmpty()) {
					throw new SQLSyntaxException("Missing parameter values for the following parameters: " + missingParams);
				}
				
				int p = 1;
				for (String paramLabel : paramLabels) { // This will be correctly ordered since we use LinkedHashSet
					Object value = paramBatch.get(paramLabel);
					ps.setString(p++, serializer.serialize(value));
				}
				
				ps.addBatch();
			}
			catch (SQLException e) {
				throw new SQLSyntaxException(e);
			}
		}
	}
	
}
