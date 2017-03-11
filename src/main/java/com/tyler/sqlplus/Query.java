package com.tyler.sqlplus;

import com.tyler.sqlplus.conversion.ConversionRegistry;
import com.tyler.sqlplus.conversion.SQLConverter;
import com.tyler.sqlplus.exception.NoResultsException;
import com.tyler.sqlplus.exception.NonUniqueResultException;
import com.tyler.sqlplus.exception.QueryStructureException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.function.BatchConsumer;
import com.tyler.sqlplus.function.Functions;
import com.tyler.sqlplus.mapper.ResultStream;
import com.tyler.sqlplus.mapper.RowMapper;
import com.tyler.sqlplus.mapper.RowMapperFactory;
import com.tyler.sqlplus.utility.Fields;
import javassist.util.proxy.Proxy;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Represents an SQL query, including both the raw SQL and all parameters.
 * <br/><br/>
 * This class provides functions for interpreting the query as a list or unique result of a given java type.
 */
public class Query {

	private static final String REGEX_PARAM = ":\\w+|\\?";

	/** The current session which constructed this query */
	private Session session;

	/** The raw SQl for this query */
	private String sql;

	/** The current parameter batch of this query. Queries may have 1 to many parameter batches */
	private LinkedHashMap<Integer, Object> currentParamBatch = new LinkedHashMap<>();

	/** All parameter batches for this query */
	private List<LinkedHashMap<Integer, Object>> paramBatches = new ArrayList<>();

	/**
	 * A mapping of parameter labels to their corresponding ordinal indices in this query.
	 * <br/>
	 * Queries may contain both string parameter labels and raw '?' parameter labels. For each query, a mapping
	 * is constructed to associate parameter labels to their respective indices
	 */
	private Map<String, Integer> paramLabel_paramIndex = new HashMap<>();

	/** Conversion registry for this query. By default, this field will be set to the default conversion registry singleton instance */
	private ConversionRegistry conversionRegistry = ConversionRegistry.getDefault();
	
	/** Should only be constructed by the Session class */
	Query(String sql, Session session) {
		this.session = session;
		this.sql = sql;
		this.paramLabel_paramIndex = parseParams(sql);
	}
	
	public Query setParameter(int index, Object val) {
		if (index > paramLabel_paramIndex.size()) {
			throw new QueryStructureException(
				"Parameter index " + index + " is out of range of this query's parameters (max parameters: " + paramLabel_paramIndex.size() + ")");
		}
		return setParameter(index + "", val);
	}
	
	public Query setParameter(String key, Object val) {
		if (!paramLabel_paramIndex.containsKey(key)) {
			throw new QueryStructureException("Unknown query parameter: " + key);
		}
		int paramIndex = paramLabel_paramIndex.get(key);
		currentParamBatch.put(paramIndex, val);
		return this;
	}

	/**
	 * Executes this query, mapping the single result to an instance of the given POJO class.
	 * @throws NonUniqueResultException If more than 1 result is returned
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
	 * Executes this query, mapping the results to a list of maps
	 */
	public List<Map<String, Object>> fetch() {
		Object result = fetchAs(Map.class);
		return (List<Map<String, Object>>) result;
	}

	/**
	 * Executes this query, mapping the results to the given POJO class
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
					throw new SQLRuntimeException(e);
				}
				batch.clear();
			}
		});

		// Will have leftover if batch size does not evenly divide into total results
		if (!batch.isEmpty()) {
			try {
				processor.acceptBatch(batch);
			} catch (Exception e) {
				throw new SQLRuntimeException(e);
			}
		}
	}

	public <T> Stream<T> streamAs(Class<T> klass) {
		RowMapper<T> mapper = RowMapperFactory.newMapper(klass, conversionRegistry, session);
		return stream().map(rs -> {
			try {
				return mapper.map(rs);
			} catch (SQLException e) {
				throw new SQLRuntimeException(e);
			}
		});
	}
	
	public Stream<ResultSet> stream() {
		try {
			PreparedStatement ps = prepareStatement(false);
			return ResultStream.stream(ps.executeQuery());
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}

	/**
	 * Execute this query's payload as an update statement, returning an array of update counts for each batched statement
	 */
	public int[] executeUpdate() {
		try {
			PreparedStatement ps = prepareStatement(false);

			int[] affectedRowsPerBatch;
			if (paramBatches.size() > 1) {
				affectedRowsPerBatch = ps.executeBatch();
			} else {
				affectedRowsPerBatch = new int[]{ ps.executeUpdate() };
			}

			return affectedRowsPerBatch;
		}
		catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
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
			
			SQLConverter<T> converter = conversionRegistry.getConverter(targetKeyClass);
			while (rsKeys.next()) {
				keys.add(converter.read(rsKeys, 1, targetKeyClass));
			}

			return keys;
		}
		catch (SQLException e) {
			throw new SQLRuntimeException(e);
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
		
		if (!currentParamBatch.isEmpty()) {
			finishBatch();
		}
		
		if (paramBatches.isEmpty() && !paramLabel_paramIndex.isEmpty()) {
			throw new QueryStructureException("No parameters set");
		}
		
		String formattedSql = getFormattedSQL();
		PreparedStatement ps = Functions.runSQL(() -> session.conn.prepareStatement(formattedSql, returnKeys ? Statement.RETURN_GENERATED_KEYS : 0));
			
		paramBatches.forEach(paramBatch -> {

			paramBatch.forEach((paramIndex, objParam) -> {
				if (objParam == null) {
					Functions.runSQL(() -> ps.setObject(paramIndex, null));
				}
				else {
					SQLConverter converter = conversionRegistry.getConverter(objParam.getClass());
					Functions.runSQL(() -> converter.write(ps, paramIndex, objParam));
				}
			});
			
			if (this.paramBatches.size() > 1) {
				Functions.runSQL(() -> ps.addBatch());
			}
		});
		
		return ps;
	}
	
	/**
	 * Binds the parameters in the given POJO class to the current parameter batch for his query
	 */
	public Query bind(Object o) {
		
		Class<?> klass = o.getClass();
		
		// Proxies are bound when lazy loading related entities. In this case, we want to pull bind fields from the proxy's superclass
		if (Proxy.class.isAssignableFrom(klass)) {
			klass = klass.getSuperclass();
		}

		boolean isCompleteBatch = true;
		for (String paramLabel : paramLabel_paramIndex.keySet()) {
			
			Field mappedField;
			try {
				mappedField = klass.getDeclaredField(paramLabel);
			} catch (NoSuchFieldException e) {
				isCompleteBatch = false;
				continue; // This parameter will need to be set manually
			}
			
			Object member = Fields.get(mappedField, o);
			Integer paramIndex = paramLabel_paramIndex.get(paramLabel);
			currentParamBatch.put(paramIndex, member);
		}

		if (isCompleteBatch) {
			finishBatch();
		}

		return this;
	}

	/**
	 * Finishes and validates the current running manual parameter batch
	 */
	public Query finishBatch() {

		List<String> missingParams = new ArrayList<>();
		paramLabel_paramIndex.forEach((param, index) -> {
			if (!currentParamBatch.containsKey(index)) {
				missingParams.add(param);
			}
		});
		if (!missingParams.isEmpty()) {
			throw new QueryStructureException("Missing parameter values for the following parameters: " + missingParams);
		}

		paramBatches.add(currentParamBatch);

		currentParamBatch = new LinkedHashMap<>();
		return this;
	}
	
	/**
	 * Formats this query's SQL by replacing all parameter labels with '?'
	 */
	private String getFormattedSQL() {
		return sql.replaceAll(REGEX_PARAM, "?");
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
					throw new QueryStructureException("Duplicate parameter '" + paramLabel + "' in query:\n" + sql);
				}
				paramLabel_index.put(paramLabel, paramIndex);
			}
		}
		
		return paramLabel_index;
	}

	@Override
	public int hashCode() {
		return Objects.hash(getFormattedSQL(), getParameterValues());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof Query) {
			Query other = (Query) o;
			return Objects.equals(getFormattedSQL(), other.getFormattedSQL()) &&
			       Objects.equals(getParameterValues(), other.getParameterValues());
		}
		return false;
	}

	@Override
	public String toString() {
		return sql;
	}

	private Collection<Object> getParameterValues() {
		Collection<Object> values = new ArrayList<>();
		paramBatches.forEach(batch -> values.addAll(batch.values()));
		values.addAll(currentParamBatch.values());
		return values;
	}

}
