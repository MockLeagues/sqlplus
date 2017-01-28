package com.tyler.sqlplus.conversion;

import com.tyler.sqlplus.exception.ConversionException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ConversionRegistry {

	private static final Map<Class<?>, FieldReader<?>> READER_REGISTRY = new LinkedHashMap<>();
	private static final Map<Class<?>, FieldWriter<?>> WRITER_REGISTRY = new LinkedHashMap<>();
	
	static {

		registerStandardReader(int.class, (rs, col, type) -> rs.getInt(col));
		registerStandardWriter(int.class, (ps, i, o) -> ps.setInt(i, o));
		
		registerStandardReader(Integer.class, (rs, col, type) -> {
			int obj = rs.getInt(col);
			return rs.wasNull() ? null : obj;
		});
		
		registerStandardWriter(Integer.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.INTEGER);
			}
			else {
				ps.setInt(i, o);
			}
		});
		
		registerStandardReader(short.class, (rs, col, type) -> rs.getShort(col));
		registerStandardWriter(short.class, (ps, i, o) -> ps.setShort(i, o));
		
		registerStandardReader(Short.class, (rs, col, type) -> {
			short obj = rs.getShort(col);
			return rs.wasNull() ? null : obj;
		});
		
		registerStandardWriter(Short.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.INTEGER);
			}
			else {
				ps.setShort(i, o);
			}
		});
		
		registerStandardReader(long.class, (rs, col, type) -> rs.getLong(col));
		registerStandardWriter(long.class, (ps, i, o) -> ps.setLong(i, o));
		
		registerStandardReader(Long.class, (rs, col, type) -> {
			long obj = rs.getLong(col);
			return rs.wasNull() ? null : obj;
		});
		
		registerStandardWriter(Long.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.INTEGER);
			}
			else {
				ps.setLong(i, o);
			}
		});
		
		registerStandardReader(float.class, (rs, col, type) -> rs.getFloat(col));
		registerStandardWriter(float.class, (ps, i, o) -> ps.setFloat(i, o));
		
		registerStandardReader(Float.class, (rs, col, type) -> {
			float obj = rs.getFloat(col);
			return rs.wasNull() ? null : obj;
		});
		
		registerStandardWriter(Float.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.FLOAT);
			}
			else {
				ps.setFloat(i, o);
			}
		});

		registerStandardReader(double.class, (rs, col, type) -> rs.getDouble(col));
		registerStandardWriter(double.class, (ps, i, o) -> ps.setDouble(i, o));
		
		registerStandardReader(Double.class, (rs, col, type) -> {
			double obj = rs.getDouble(col);
			return rs.wasNull() ? null : obj;
		});
		
		registerStandardWriter(Double.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.DOUBLE);
			}
			else {
				ps.setDouble(i, o);
			}
		});
		
		registerStandardReader(boolean.class, (rs, col, type) -> rs.getBoolean(col));
		registerStandardWriter(boolean.class, (ps, i, o) -> ps.setBoolean(i, o));
		
		registerStandardReader(Boolean.class, (rs, col, type) -> {
			boolean obj = rs.getBoolean(col);
			return rs.wasNull() ? null : obj;
		});
		
		registerStandardWriter(Boolean.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.BOOLEAN);
			}
			else {
				ps.setBoolean(i, o);
			}
		});
		
		registerStandardReader(BigInteger.class, (rs, col, type) -> {
			BigDecimal bDec = rs.getBigDecimal(col);
			return bDec == null ? null : bDec.toBigInteger(); 
		});
		
		registerStandardWriter(BigInteger.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.BIGINT);
			}
			else {
				ps.setBigDecimal(i, new BigDecimal(o));
			}
		});
		
		registerStandardReader(BigDecimal.class, (rs, col, type) -> rs.getBigDecimal(col));
		
		registerStandardWriter(BigDecimal.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.DECIMAL);
			}
			else {
				ps.setBigDecimal(i, o);
			}
		});
		
		registerStandardReader(char.class, (rs, col, type) -> {
			String str = rs.getString(col);
			if (rs.wasNull()) {
				return Character.MIN_VALUE;
			}
			else {
				if (str.length() > 1) {
					throw new ConversionException("Failed to read char value from string '" + str + "'; length is greater than 1 character");
				}
				return str.charAt(0);
			}
		});
		
		registerStandardWriter(char.class, (ps, i, o) -> ps.setString(i, String.valueOf(o)));
		
		registerStandardReader(Character.class, (rs, col, type) -> {
			String str = rs.getString(col);
			if (rs.wasNull()) {
				return null;
			}
			else {
				if (str.length() > 1) {
					throw new ConversionException("Failed to read Character value from string '" + str + "'; length is greater than 1 character");
				}
				return str.charAt(0);
			}
		});
		
		registerStandardWriter(Character.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.CHAR);
			}
			else {
				ps.setString(i, String.valueOf(o));
			}
		});
		
		registerStandardReader(String.class, (rs, col, type) -> rs.getString(col));
		registerStandardWriter(String.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.VARCHAR);
			}
			else {
				ps.setString(i, o);
			}
		});
		
		registerStandardReader(Date.class, (rs, col, type)  -> {
			Object dbObj = rs.getObject(col);
			if (dbObj.getClass() == java.sql.Date.class) {
				return new Date(((java.sql.Date)dbObj).getTime());
			}
			else if (dbObj.getClass() == Timestamp.class) {
				return new Date(((Timestamp)dbObj).getTime());
			}
			else if (dbObj.getClass() == java.sql.Time.class) {
				throw new UnsupportedOperationException("Cannot convert time field to java.util.Date");
			}
			else {
				throw new ConversionException("Could not extract Date value from ResultSet object " + dbObj);
			}
		});
		
		registerStandardWriter(Date.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.DATE);
			}
			else {
				ps.setDate(i, new java.sql.Date(o.getTime()));
			}
		});
		
		registerStandardReader(Timestamp.class, (rs, column, type) -> {
			Object dbObj = rs.getObject(column);
			if (dbObj == null) {
				return null;
			}
			else if (dbObj.getClass() == Timestamp.class) {
				return (Timestamp) dbObj;
			}
			else if (dbObj.getClass() == java.sql.Time.class) {
				throw new UnsupportedOperationException("Cannot convert time field to java.sql.Timestamp");
			}
			else if (dbObj.getClass() == java.sql.Date.class) {
				return new Timestamp(((java.sql.Date)dbObj).getTime());
			}
			else {
				throw new ConversionException("Could not extract Timestamp value from ResultSet object " + dbObj);
			}
		});
		
		registerStandardWriter(Timestamp.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.TIMESTAMP);
			}
			else {
				ps.setTimestamp(i, o);
			}
		});
		
		registerStandardReader(LocalDate.class, (rs, column, type) -> {
			Object dbObj = rs.getObject(column);
			if (dbObj != null) {
				if (dbObj.getClass() == Timestamp.class) {
					return ((Timestamp)dbObj).toLocalDateTime().toLocalDate();
				}
				else {
					return LocalDate.parse(String.valueOf(dbObj));
				}
			}
			else {
				return LocalDate.parse(rs.getString(column));
			}
		});
		
		registerStandardWriter(LocalDate.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.DATE);
			}
			else {
				ps.setString(i, o.toString());
			}
		});
		
		registerStandardReader(LocalDateTime.class, (rs, column, type) -> {
			Object dbObj = rs.getObject(column);
			if (dbObj == null) {
				return LocalDateTime.parse(rs.getString(column));
			}
			else {
				if (dbObj.getClass() == Timestamp.class) {
					return ((Timestamp)dbObj).toLocalDateTime();
				}
				else if (dbObj.getClass() == java.sql.Date.class) {
					return ((java.sql.Date)dbObj).toLocalDate().atStartOfDay();
				}
				else {
					return LocalDateTime.parse(String.valueOf(dbObj));
				}
			}
		});
		
		registerStandardWriter(LocalDateTime.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.DATE);
			}
			else {
				ps.setString(i, o.toString());
			}
		});
		
		registerStandardReader(LocalTime.class, (rs, column, type) -> {
			Object dbObj = rs.getObject(column);
			if (dbObj == null) {
				return LocalTime.parse(rs.getString(column));
			}
			if (dbObj.getClass() == Timestamp.class) {
				return ((Timestamp)dbObj).toLocalDateTime().toLocalTime();
			}
			else if (dbObj.getClass() == java.sql.Date.class) {
				throw new UnsupportedOperationException(
					"Cannot convert date field to java.time.LocalTime field; date fields do not contain time components");
			}
			else {
				return LocalTime.parse(String.valueOf(dbObj));
			}
		});
		
		registerStandardWriter(LocalTime.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.TIME);
			}
			else {
				ps.setString(i, o.toString());
			}
		});
		
		registerStandardReader(Enum.class, (rs, column, type) -> {
			try {
				Method valueOf = type.getDeclaredMethod("valueOf", String.class);
				valueOf.setAccessible(true);
				return (Enum<?>) valueOf.invoke(null, rs.getString(column));
			} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		});
		
		registerStandardWriter(Enum.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.VARCHAR);
			}
			else {
				ps.setString(i, ((Enum<?>) o).name());
			}
		});
		
		registerStandardReader(Object.class, (rs, col, type) -> rs.getObject(col));
		registerStandardWriter(Object.class, (ps, i, o) -> ps.setObject(i, o));
	}

	public static boolean containsStandardReader(Class<?> type) {
		return READER_REGISTRY.containsKey(type);
	}
	
	public static <T> void registerStandardReader(Class<T> type, FieldReader<T> reader) {
		READER_REGISTRY.put(type, reader);
	}
	
	public static <T> void registerStandardWriter(Class<T> type, FieldWriter<T> writer) {
		WRITER_REGISTRY.put(type, writer);
	}
	
	private Map<Class<?>, FieldReader<?>> readers;
	private Map<Class<?>, FieldWriter<?>> writers;
	
	public ConversionRegistry() {
		readers = new HashMap<>(READER_REGISTRY);
		writers = new HashMap<>(WRITER_REGISTRY);
	}
	
	public <T> void registerReader(Class<T> type, FieldReader<T> reader) {
		this.readers.put(type, reader);
	}
	
	public <T> void registerWriter(Class<T> type, FieldWriter<T> writer) {
		this.writers.put(type, writer);
	}
	
	@SuppressWarnings("unchecked")
	public <T> FieldReader<T> getReader(Class<T> type) {
		
		return (FieldReader<T>) readers.computeIfAbsent(type, t -> {
			Class<?> registeredSupertype = readers.keySet()
			                                      .stream()
			                                      .filter(registeredType -> registeredType.isAssignableFrom(type))
			                                      .findFirst()
			                                      .orElseThrow(() -> new ConversionException("No suitable reader found for  " + type));
			return readers.get(registeredSupertype);
		});
		
	}
	
	@SuppressWarnings("unchecked")
	public <T> FieldWriter<T> getWriter(Class<T> type) {
		
		return (FieldWriter<T>) writers.computeIfAbsent(type, t -> {
			Class<?> registeredSupertype = writers.keySet()
			                                      .stream()
			                                      .filter(registeredType -> registeredType.isAssignableFrom(type))
			                                      .findFirst()
			                                      .orElseThrow(() -> new ConversionException("No suitable writer found for  " + type));
			return writers.get(registeredSupertype);
		});
	}
	
}
