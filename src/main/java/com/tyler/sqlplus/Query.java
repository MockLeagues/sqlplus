package com.tyler.sqlplus;

import com.tyler.sqlplus.conversion.ConversionRegistry;
import com.tyler.sqlplus.conversion.FieldReader;
import com.tyler.sqlplus.conversion.FieldWriter;
import com.tyler.sqlplus.exception.*;
import com.tyler.sqlplus.function.BatchConsumer;
import com.tyler.sqlplus.function.Functions;
import com.tyler.sqlplus.mapper.ResultMapper;
import com.tyler.sqlplus.mapper.ResultMappers;
import com.tyler.sqlplus.mapper.ResultStream;
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
 * Provides encapsulation for an SQL query, allowing results to be retrieved and streamed as POJOs
 */
public class Query {

	private static final String REGEX_PARAM = ":\\w+|\\?";
	
	private Session session;
	private String sql;
	private LinkedHashMap<Integer, Object> manualParamBatch = new LinkedHashMap<>();
	private List<LinkedHashMap<Integer, Object>> paramBatches = new ArrayList<>();
	private Map<String, Integer> paramLabel_paramIndex = new HashMap<>();
	private ConversionRegistry conversionRegistry = new ConversionRegistry();
	
	/** Should only be constructed by the Session class */
	Query(String sql, Session session) {
		this.session = session;
		this.sql = sql;
		this.paramLabel_paramIndex = parseParams(sql);
	}
	
	/**
	 * Sets the function to use for reading parameter objects from the result set produced by this query. These will
	 * be used when constructing POJO objects
	 */
	public <T> Query setReader(Class<T> type, FieldReader<T> reader) {
		conversionRegistry.registerReader(type, reader);
		return this;
	}
	
	/**
	 * Sets the function to use for writing parameter objects of the given type for this query
	 */
	public <T> Query setWriter(Class<T> type, FieldWriter<T> writer) {
		conversionRegistry.registerWriter(type, writer);
		return this;
	}
	
	public Query setParameter(Integer index, Object val) {
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
		Integer paramIndex = paramLabel_paramIndex.get(key);
		manualParamBatch.put(paramIndex, val);
		return this;
	}
	
	/**
	 * Executes this query, mapping results to a simple list of maps
	 */
	public List<Map<String, Object>> fetch() {
		ResultMapper<Map<String, Object>> rowMapper = ResultMappers.forMap();
		return stream().map(rs -> Functions.runSQL(() -> rowMapper.map(rs))).collect(toList());
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
				Functions.runSQL(() -> processor.acceptBatch(batch));
				batch.clear();
			}
		});
		
		// Will have leftover if batch size does not evenly divide into total results
		if (!batch.isEmpty()) {
			Functions.runSQL(() -> processor.acceptBatch(batch));
		}
	}
	
	public <T> Stream<T> streamAs(Class<T> klass) {
		ResultMapper<T> pojoMapper = ResultMappers.forClass(klass, conversionRegistry, session);
		return stream().map(rs -> Functions.runSQL(() -> pojoMapper.map(rs)));
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
	 * Execute this query's payload as an update statement
	 */
	public void executeUpdate() {
		try {
			PreparedStatement ps = prepareStatement(false);
			if (paramBatches.size() > 1) {
				ps.executeBatch();
			} else {
				ps.executeUpdate();
			}
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
			
			FieldReader<T> reader = conversionRegistry.getReader(targetKeyClass);
			while (rsKeys.next()) {
				keys.add(reader.read(rsKeys, 1, targetKeyClass));
			}
			return keys;
					
		} catch (SQLException e) {
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
		
		if (!manualParamBatch.isEmpty()) {
			finishBatch();
		}
		
		if (this.paramBatches.isEmpty() && !paramLabel_paramIndex.isEmpty()) {
			throw new QueryStructureException("No parameters set");
		}
		
		String formattedSql = sql.replaceAll(REGEX_PARAM, "?");
		PreparedStatement ps = Functions.runSQL(() -> session.conn.prepareStatement(formattedSql, returnKeys ? Statement.RETURN_GENERATED_KEYS : 0));
			
		for (Map<Integer, Object> paramBatch : this.paramBatches) {

			paramBatch.forEach((paramIndex, objParam) -> {
				if (objParam == null) {
					Functions.runSQL(() -> ps.setObject(paramIndex, null));
				} else {
					FieldWriter writer = conversionRegistry.getWriter(objParam.getClass());
					Functions.runSQL(() -> writer.write(ps, paramIndex, objParam));
				}
			});
			
			if (this.paramBatches.size() > 1) {
				Functions.runSQL(() -> ps.addBatch());
			}
		}
		
		return ps;
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
				throw new ReflectionException("No member exists in " + klass + " to bind a value for query parameter '" + paramLabel + "'");
			}
			
			Object member = Fields.get(mappedField, o);
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
		
		List<String> missingParams = new ArrayList<>();
		paramLabel_paramIndex.forEach((param, index) -> {
			if (!newBatch.containsKey(index)) {
				missingParams.add(param);
			}
		});
		if (!missingParams.isEmpty()) {
			throw new QueryStructureException("Missing parameter values for the following parameters: " + missingParams);
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
					throw new QueryStructureException("Duplicate parameter '" + paramLabel + "' in query:\n" + sql);
				}
				paramLabel_index.put(paramLabel, paramIndex);
			}
		}
		
		return paramLabel_index;
	}
	
	@Override
	public String toString() {
		return sql;
	}
	
}
