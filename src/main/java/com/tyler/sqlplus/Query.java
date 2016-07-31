package com.tyler.sqlplus;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.tyler.sqlplus.conversion.AttributeConverter;
import com.tyler.sqlplus.conversion.ConversionPolicy;
import com.tyler.sqlplus.exception.NoResultsException;
import com.tyler.sqlplus.exception.NonUniqueResultException;
import com.tyler.sqlplus.exception.POJOBindException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.utility.ReflectionUtils;
import com.tyler.sqlplus.utility.ResultStream;

public class Query {

	private static final String REGEX_PARAM = ":\\w+|\\?";
	
	private PreparedStatement ps;
	private LinkedHashMap<Integer, Object> manualParamBatch = new LinkedHashMap<>();
	private List<LinkedHashMap<Integer, Object>> paramBatches = new ArrayList<>();
	private Map<String, Integer> paramLabel_paramIndex = new HashMap<>();
	private ConversionPolicy conversionPolicy;
	
	public Query(String sql, Connection conn) {
		try {
			this.paramLabel_paramIndex = parseParams(sql);
			this.ps = conn.prepareStatement(sql.replaceAll(REGEX_PARAM, "?"), Statement.RETURN_GENERATED_KEYS);
			this.conversionPolicy = new ConversionPolicy();
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	public <T> Query setConverter(Class<T> type, AttributeConverter<T> converter) {
		this.conversionPolicy.setConverter(type, converter);
		return this;
	}
	
	public Query setParameter(Integer index, Object val) {
		if (index > paramLabel_paramIndex.size()) {
			throw new SQLRuntimeException("Parameter index " + index + " is out of range of this query's parameters");
		}
		return setParameter(index + "", val);
	}
	
	public Query setParameter(String key, Object val) {
		if (!paramLabel_paramIndex.containsKey(key)) {
			throw new SQLRuntimeException("Unknown query parameter: " + key);
		}
		Integer paramIndex = paramLabel_paramIndex.get(key);
		manualParamBatch.put(paramIndex, val);
		return this;
	}
	
	/**
	 * Executes this query, mapping results to a simple list of maps
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String, String>> fetch() {
		Object result = fetchAs(Map.class);
		return (List<Map<String, String>>) result;
	}
	/**
	 * Executes this query, mapping the single result to an instance of the given POJO class. If more than 1 result is returned,
	 * a NonUniqueResultException will be thrown
	 */
	public <T> T getUniqueResultAs(Class<T> resultClass) {
		List<T> results = fetchAs(resultClass);
		if (results.size() != 1) {
			throw new NonUniqueResultException();
		}
		return results.get(0);
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
	
	public <T> Stream<T> streamAs(Class<T> klass) {
		try {
			applyParameterBatches();
			ResultMapper<T> pojoMapper = ResultMapper.forType(klass);
			return ResultStream.stream(ps.executeQuery()).map(rs -> {
				try {
					return pojoMapper.map(rs);
				} catch (SQLException e) {
					throw new SQLRuntimeException(e);
				}
			});
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	/**
	 * Returns the result of this query as a 'scalar' (single) value of the given Java class type.
	 * 
	 * This method will throw a SQLSyntaxException if the produced result set has more than 1 column
	 */
	public <T> T fetchScalar(Class<T> scalarClass) {
		try {
			applyParameterBatches();
			ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				throw new NoResultsException();
			}
			if (rs.getMetaData().getColumnCount() > 1) {
				throw new SQLRuntimeException("Scalar query returned more than 1 column");
			}
			return conversionPolicy.findConverter(scalarClass).get(rs, 1);
		} catch (SQLException e) {
			throw new SQLRuntimeException("Error retrieving scalar value", e);
		}
	}
	
	/**
	 * Execute this query's payload as an update statement
	 */
	public void executeUpdate() {
		executeUpdate(Object.class);
	}
	
	/**
	 * Executes this query's payload as an update statement, returning the generated keys as instances of the given class
	 */
	public <T> List<T> executeUpdate(Class<T> targetKeyClass) {
		
		try {
			applyParameterBatches();
			
			if (paramBatches.size() > 1) {
				ps.executeBatch();
			} else {
				ps.executeUpdate();
			}
			
			List<T> keys = new ArrayList<>();
			ResultSet rsKeys = ps.getGeneratedKeys();
			
			AttributeConverter<T> keyConverter = conversionPolicy.findConverter(targetKeyClass);
			while (rsKeys.next()) {
				keys.add(keyConverter.get(rsKeys, 1));
			}
			return keys;
					
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}

	/**
	 * Applies all parameter batches stored in this query to its PreparedStatement object. If there is a running manual parameter batch
	 * that has not been queued yet, that will also be added to the batch queue.
	 * 
	 * Queries which have more than 1 parameter batch will result in a call to addBatch() on the underlying PreparedStatement object for each batch.
	 * Queries with only 1 parameter batch will simply apply each parameter in the batch and then return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void applyParameterBatches() {
		
		if (!manualParamBatch.isEmpty()) {
			finishBatch();
		}
		
		if (this.paramBatches.isEmpty() && !paramLabel_paramIndex.isEmpty()) {
			throw new SQLRuntimeException("No parameters set");
		}
		
		try {
			
			for (Map<Integer, Object> paramBatch : this.paramBatches) {
			
				for (Map.Entry<Integer, Object> e : paramBatch.entrySet()) {
					Object objParam = e.getValue();
					Integer paramIndex = e.getKey();
					AttributeConverter converter = conversionPolicy.findConverter(objParam.getClass());
					converter.set(ps, paramIndex, objParam);
				}
				
				if (this.paramBatches.size() > 1) {
					ps.addBatch();
				}
			}
		}
		catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	/**
	 * Binds the parameters in the given POJO class as a batch on this query
	 */
	public Query bind(Object o) {
		
		Class<?> klass = o.getClass();
		LinkedHashMap<Integer, Object> bindParams = new LinkedHashMap<>();
		for (String paramLabel : paramLabel_paramIndex.keySet()) {
			
			Field mappedField;
			try {
				mappedField = klass.getDeclaredField(paramLabel);
			} catch (NoSuchFieldException e) {
				throw new POJOBindException("No member exists in class " + o.getClass().getName() + " to bind a value for parameter '" + paramLabel + "'");
			}
			
			Object member = null;
			try {
				member = ReflectionUtils.get(mappedField, o);
			}
			catch (IllegalArgumentException | IllegalAccessException e) {
				throw new POJOBindException(e);
			}
			
			Integer paramIndex = paramLabel_paramIndex.get(paramLabel);
			bindParams.put(paramIndex, member);
		}
			
		addBatch(bindParams);
		return this;
	}

	/**
	 * Finishes and validates the current running manual parameter batch
	 */
	public Query finishBatch() {
		addBatch(manualParamBatch);
		manualParamBatch = new LinkedHashMap<>();
		return this;
	}
	
	/**
	 * Adds a parameter batch to the list of this querie's batches.
	 * Verifies a parameter exists in the given batch for each given parameter label. If not, a SQLRuntimeException is thrown
	 */
	private void addBatch(LinkedHashMap<Integer, Object> newBatch) {
		
		// See if we are missing any labeled parameters
		List<String> missingParams = new ArrayList<>();
		paramLabel_paramIndex.forEach((param, index) -> {
			if (!newBatch.containsKey(index)) {
				missingParams.add(param);
			}
		});
		if (!missingParams.isEmpty()) {
			throw new SQLRuntimeException("Missing parameter values for the following parameters: " + missingParams);
		}
		
		paramBatches.add(newBatch);
	}

	/**
	 * Produces a mapping of parameter labels to the 1-based index at which they appear in the given query string.
	 * 
	 * For any '?' params, the key will be equal to the string value of the index. For example, for the query
	 * 'select fieldA from table1 where fieldA = ? and fieldB = ?', a mapping would be produced with the keys
	 * "1" and "2" and the values 1 and 2.
	 */
	private static Map<String, Integer> parseParams(String sql) {
		
		Map<String, Integer> paramLabel_index = new HashMap<>();
		
		int paramIndex = 0;
		Matcher paramsMatcher = Pattern.compile(REGEX_PARAM).matcher(sql);
		while (paramsMatcher.find()) {
			paramIndex++;
			String paramLabel = paramsMatcher.group();
			if (paramLabel.equals("?")) {
				paramLabel_index.put(paramIndex + "", paramIndex);
			}
			else {
				paramLabel_index.put(paramLabel.substring(1), paramIndex);
			}
		}
		
		return paramLabel_index;
	}
	
}
