package com.tyler.sqlplus;

import static java.util.stream.Collectors.toList;

import java.lang.reflect.Field;
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
import java.util.stream.Stream;

import com.tyler.sqlplus.conversion.AttributeConverter;
import com.tyler.sqlplus.conversion.ConversionPolicy;
import com.tyler.sqlplus.exception.NoResultsException;
import com.tyler.sqlplus.exception.NonUniqueResultException;
import com.tyler.sqlplus.exception.POJOBindException;
import com.tyler.sqlplus.exception.QuerySyntaxException;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.exception.SqlRuntimeException;
import com.tyler.sqlplus.functional.BatchConsumer;
import com.tyler.sqlplus.utility.Fields;
import com.tyler.sqlplus.utility.ResultStream;

import javassist.util.proxy.Proxy;

/**
 * Provides encapsulation for an SQL query, allowing results to be retrieved and streamed as POJOs
 * @author Tyler
 *
 */
public class Query {

	private static final String REGEX_PARAM = ":\\w+|\\?";
	
	private Session session;
	private String sql;
	private boolean underscoreCamelCaseConvert = false;
	private LinkedHashMap<Integer, Object> manualParamBatch = new LinkedHashMap<>();
	private List<LinkedHashMap<Integer, Object>> paramBatches = new ArrayList<>();
	private Map<String, Integer> paramLabel_paramIndex = new HashMap<>();
	private Map<String, String> rsColumn_classFieldName = new HashMap<>();
	private ConversionPolicy conversionPolicy = new ConversionPolicy();
	
	public Query(String sql, Session session) {
		this.session = session;
		this.sql = sql;
		this.paramLabel_paramIndex = parseParams(sql);
	}
	
	public <T> Query setConverter(Class<T> type, AttributeConverter<T> converter) {
		this.conversionPolicy.setConverter(type, converter);
		return this;
	}
	
	public Query setParameter(Integer index, Object val) {
		if (index > paramLabel_paramIndex.size()) {
			throw new QuerySyntaxException(
				"Parameter index " + index + " is out of range of this query's parameters (max parameters: " + paramLabel_paramIndex.size() + ")");
		}
		return setParameter(index + "", val);
	}
	
	public Query addColumnMapping(String resultSetColumnName, String classFieldName) {
		rsColumn_classFieldName.put(resultSetColumnName, classFieldName);
		return this;
	}
	
	public Query setParameter(String key, Object val) {
		if (!paramLabel_paramIndex.containsKey(key)) {
			throw new QuerySyntaxException("Unknown query parameter: " + key);
		}
		Integer paramIndex = paramLabel_paramIndex.get(key);
		manualParamBatch.put(paramIndex, val);
		return this;
	}
	
	/**
	 * Sets whether the column names of this query's result set should be converted from underscore to camel-case when mapping
	 * them to POJO fields
	 */
	public Query setConvertUnderscoreToCamelCase(boolean convert) {
		this.underscoreCamelCaseConvert = convert;
		return this;
	}
	
	/**
	 * Executes this query, mapping results to a simple list of maps
	 */
	public List<Map<String, Object>> fetch() {
		ResultMapper<Map<String, Object>> rowMapper = ResultMapper.forMap();
		return stream().map(rs -> {
			try {
				return rowMapper.map(rs);
			}
			catch (SQLException e) {
				throw new SqlRuntimeException(e);
			}
		}).collect(toList());
	}
	/**
	 * Executes this query, mapping the single result to an instance of the given POJO class. If more than 1 result is returned,
	 * a NonUniqueResultException will be thrown
	 */
	public <T> T getUniqueResultAs(Class<T> resultClass) {
		List<T> results = fetchAs(resultClass);
		if (results.isEmpty()) {
			throw new NoResultsException();
		}
		if (results.size() > 1) {
			throw new NonUniqueResultException();
		}
		return results.get(0);
	}
	
	/**
	 * Executes this query, mapping the results to the given POJO class. ResultSet columns will directly map
	 * to the POJO's field names unless custom field mappings are present for this query or custom column names
	 * are specified in 'as' clauses
	 */
	public <T> List<T> fetchAs(Class<T> resultClass) {
		return streamAs(resultClass).collect(toList());
	}
	
	/**
	 * Processes results in batches of the given size.
	 * 
	 * This method is useful for processing huge chunks of data which could potentially exhaust available memory if read all at once
	 */
	public <T> void batchProcess(Class<T> batchType, int batchSize, BatchConsumer<T> processor) {
		
		List<T> batch = new ArrayList<>();
		streamAs(batchType).forEach(data -> {
			batch.add(data);
			if (batch.size() == batchSize) {
				try {
					processor.acceptBatch(batch);
				} catch (Exception e) {
					throw new SqlRuntimeException(e);
				}
				batch.clear();
			}
		});
		
		// Will have leftover if batch size does not evenly divide into total results
		if (!batch.isEmpty()) {
			try {
				processor.acceptBatch(batch);
			} catch (Exception e) {
				throw new SqlRuntimeException(e);
			}
		}
	}
	
	public <T> Stream<T> streamAs(Class<T> klass) {
		ResultMapper<T> pojoMapper = ResultMapper.forType(klass, conversionPolicy, rsColumn_classFieldName, session, underscoreCamelCaseConvert);
		return stream().map(rs -> {
			try {
				return pojoMapper.map(rs);
			} catch (SQLException e) {
				throw new POJOBindException(e);
			}
		});
	}
	
	public <T> Stream<ResultSet> stream() {
		try {
			PreparedStatement ps = prepareStatement(false);
			return ResultStream.stream(ps.executeQuery());
		} catch (SQLException e) {
			throw new SqlRuntimeException(e);
		}
	}
	
	/**
	 * Returns the result of this query as a 'scalar' (single) value of the given Java class type.
	 * 
	 * This method will throw a SQLRuntimeException if the produced result set has more than 1 column
	 */
	public <T> T fetchScalar(Class<T> scalarClass) {
		try {
			PreparedStatement ps = prepareStatement(false);
			ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				throw new NoResultsException();
			}
			if (rs.getMetaData().getColumnCount() > 1) {
				throw new SqlRuntimeException("Scalar query returned more than 1 column");
			}
			return conversionPolicy.findConverter(scalarClass).get(rs, 1);
		} catch (SQLException e) {
			throw new SqlRuntimeException("Error retrieving scalar value", e);
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
			
			PreparedStatement ps = prepareStatement(true);
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
			throw new SqlRuntimeException(e);
		}
	}

	/**
	 * Creates a PreparedStatement and then applies all parameter batches stored in this query to it. If there is a running manual parameter batch
	 * that has not been queued yet, that will also be added to the batch queue.
	 * 
	 * Queries which have more than 1 parameter batch will result in a call to addBatch() on the underlying PreparedStatement object for each batch.
	 * Queries with only 1 parameter batch will simply apply each parameter in the batch and then return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private PreparedStatement prepareStatement(boolean returnKeys) {
		
		if (!manualParamBatch.isEmpty()) {
			finishBatch();
		}
		
		if (this.paramBatches.isEmpty() && !paramLabel_paramIndex.isEmpty()) {
			throw new QuerySyntaxException("No parameters set");
		}
		
		try {
		
			String formattedSql = sql.replaceAll(REGEX_PARAM, "?");
			PreparedStatement ps;
			if (returnKeys) {
				ps = session.getJdbcConnection().prepareStatement(formattedSql, Statement.RETURN_GENERATED_KEYS);
			}
			else {
				ps = session.getJdbcConnection().prepareStatement(formattedSql);
			}
			
			for (Map<Integer, Object> paramBatch : this.paramBatches) {
			
				for (Map.Entry<Integer, Object> e : paramBatch.entrySet()) {
					Object objParam = e.getValue();
					Integer paramIndex = e.getKey();
					if (objParam == null) {
						ps.setObject(paramIndex, null);
					}
					else {
						AttributeConverter converter = conversionPolicy.findConverter(objParam.getClass());
						converter.set(ps, paramIndex, objParam);
					}
				}
				
				if (this.paramBatches.size() > 1) {
					ps.addBatch();
				}
			}
			
			return ps;
		}
		catch (SQLException e) {
			throw new SqlRuntimeException(e);
		}
	}
	
	/**
	 * Binds the parameters in the given POJO class as a batch on this query
	 */
	public Query bind(Object o) {
		
		Class<?> klass = o.getClass();
		
		// Proxies are bound when lazy loading related entities. In this case, we want to pull bind fields from the proxy's superclass
		if (Proxy.class.isAssignableFrom(klass)) {
			klass = klass.getSuperclass();
		}
		
		LinkedHashMap<Integer, Object> bindParams = new LinkedHashMap<>();
		for (String paramLabel : paramLabel_paramIndex.keySet()) {
			
			Field mappedField;
			try {
				mappedField = klass.getDeclaredField(paramLabel);
			} catch (NoSuchFieldException e) {
				throw new POJOBindException("No member exists in class " + klass.getName() + " to bind a value for parameter '" + paramLabel + "'");
			}
			
			Object member = null;
			try {
				member = Fields.get(mappedField, o);
			}
			catch (ReflectionException e) {
				throw new POJOBindException("Error retrieving value for bind field " + mappedField, e);
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
	 * Verifies a parameter exists in the given batch for each given parameter label. If not, a QuerySyntaxException is thrown
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
			throw new QuerySyntaxException("Missing parameter values for the following parameters: " + missingParams);
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
				paramLabel = paramLabel.substring(1);
				if (paramLabel_index.containsKey(paramLabel)) {
					throw new QuerySyntaxException("Duplicate parameter '" + paramLabel + "'");
				}
				paramLabel_index.put(paramLabel, paramIndex);
			}
		}
		
		return paramLabel_index;
	}
	
}
