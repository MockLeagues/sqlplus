package com.tyler.sqlplus.conversion;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.tyler.sqlplus.exception.ConversionException;

public class ConversionRegistry {

	public static final DbReader<Integer> INT_READER = (rs, col)  -> rs.getInt(col);
	public static final DbWriter<Integer> INT_WRITER = (ps, i, o) -> ps.setInt(i, o);
	
	public static final DbReader<Short> SHORT_READER = (rs, col)  -> rs.getShort(col);
	public static final DbWriter<Short> SHORT_WRITER = (ps, i, o) -> ps.setShort(i, o);
	
	public static final DbReader<Long> LONG_READER = (rs, col)  -> rs.getLong(col);
	public static final DbWriter<Long> LONG_WRITER = (ps, i, o) -> ps.setLong(i, o);
	
	public static final DbReader<Float> FLOAT_READER = (rs, col)  -> rs.getFloat(col);
	public static final DbWriter<Float> FLOAT_WRITER = (ps, i, o) -> ps.setFloat(i, o);
	
	public static final DbReader<Double> DOUBLE_READER = (rs, col)  -> rs.getDouble(col);
	public static final DbWriter<Double> DOUBLE_WRITER = (ps, i, o) -> ps.setDouble(i, o);
	
	public static final DbReader<Boolean> BOOLEAN_READER = (rs, col)  -> rs.getBoolean(col);
	public static final DbWriter<Boolean> BOOLEAN_WRITER = (ps, i, o) -> ps.setBoolean(i, o);
	
	public static final DbReader<String> STRING_READER = (rs, col)  -> rs.getString(col);
	public static final DbWriter<String> STRING_WRITER = (ps, i, o) -> ps.setString(i, o);
	
	public static final DbReader<Object> OBJECT_READER = (rs, col)  -> rs.getObject(col);
	public static final DbWriter<Object> OBJECT_WRITER = (ps, i, o) -> ps.setObject(i, o);
	
	public static final DbReader<Character> CHAR_READER = (rs, col)  -> {
		String str = rs.getString(col);
		return str == null || str.isEmpty() ? null : str.charAt(0);
	};
	
	public static final DbWriter<Character> CHAR_WRITER = (ps, i, o) -> ps.setString(i, new String(new char[]{o}));
	
	public static final DbReader<Date> DATE_READER = (rs, col)  -> {
		Object dbObj = rs.getObject(col);
		if (dbObj.getClass() == Timestamp.class) {
			return new Date(((Timestamp)dbObj).getTime());
		}
		else if (dbObj.getClass() == java.sql.Time.class) {
			throw new UnsupportedOperationException("Cannot convert time field to java.util.Date");
		}
		return rs.getDate(col);
	};
	
	public static final DbWriter<Date> DATE_WRITER = (ps, i, o) -> ps.setDate(i, new java.sql.Date(o.getTime()));

	public static final DbReader<Timestamp> TIMESTAMP_READER = (rs, column) -> {
		Object dbObj = rs.getObject(column);
		if (dbObj.getClass() == java.sql.Time.class) {
			throw new UnsupportedOperationException("Cannot convert time field to java.sql.Timestamp");
		}
		return rs.getTimestamp(column);
	};
	
	public static final DbWriter<Timestamp> TIMESTAMP_WRITER = (ps, i, o) -> ps.setTimestamp(i, o);
	
	public static final DbReader<LocalDate> LOCAL_DATE_READER = (rs, column) -> {
		Object dbObj = rs.getObject(column);
		if (dbObj != null) {
			if (dbObj.getClass() == Timestamp.class) {
				return ((Timestamp)dbObj).toLocalDateTime().toLocalDate();
			}
			else {
				return LocalDate.parse(rs.getString(column));
			}
		}
		else {
			return LocalDate.parse(rs.getString(column));
		}
	};
	
	public static final DbWriter<LocalDate> LOCAL_DATE_WRITER = (ps, i, o) -> ps.setString(i, o.toString());
	
	public static final DbReader<LocalDateTime> LOCAL_DATE_TIME_READER = (rs, column) -> {
		Object dbObj = rs.getObject(column);
		if (dbObj.getClass() == Timestamp.class) {
			return ((Timestamp)dbObj).toLocalDateTime();
		}
		else if (dbObj.getClass() == java.sql.Date.class) {
			return ((java.sql.Date)dbObj).toLocalDate().atStartOfDay();
		}
		return LocalDateTime.parse(rs.getString(column));
	};
	
	public static final DbWriter<LocalDateTime> LOCAL_DATE_TIME_WRITER = (ps, i, o) -> ps.setString(i, o.toString());
	
	public static final DbReader<LocalTime> LOCAL_TIME_READER = (rs, column) -> {
		Object dbObj = rs.getObject(column);
		if (dbObj.getClass() == Timestamp.class) {
			return ((Timestamp)dbObj).toLocalDateTime().toLocalTime();
		}
		else if (dbObj.getClass() == java.sql.Date.class) {
			throw new UnsupportedOperationException(
				"Cannot convert date field to java.time.LocalTime field; date fields do not contain time components");
		}
		return LocalTime.parse(rs.getString(column));
	};
	
	public static final DbWriter<LocalTime> LOCAL_TIME_WRITER = (ps, i, o) -> ps.setString(i, o.toString());
	
	private static final Map<Class<?>, DbReader<?>> READER_REGISTRY = new HashMap<>();
	private static final Map<Class<?>, DbWriter<?>> WRITER_REGISTRY = new HashMap<>();
	static {
		
		registerStandardReader(int.class, INT_READER);
		registerStandardWriter(int.class, INT_WRITER);
		
		registerStandardReader(Integer.class, INT_READER);
		registerStandardWriter(Integer.class, INT_WRITER);
		
		registerStandardReader(short.class, SHORT_READER);
		registerStandardWriter(short.class, SHORT_WRITER);
		
		registerStandardReader(Short.class, SHORT_READER);
		registerStandardWriter(Short.class, SHORT_WRITER);
		
		registerStandardReader(long.class, LONG_READER);
		registerStandardWriter(long.class, LONG_WRITER);
		
		registerStandardReader(Long.class, LONG_READER);
		registerStandardWriter(Long.class, LONG_WRITER);
		
		registerStandardReader(float.class, FLOAT_READER);
		registerStandardWriter(float.class, FLOAT_WRITER);
		
		registerStandardReader(Float.class, FLOAT_READER);
		registerStandardWriter(Float.class, FLOAT_WRITER);

		registerStandardReader(double.class, DOUBLE_READER);
		registerStandardWriter(double.class, DOUBLE_WRITER);
		
		registerStandardReader(Double.class, DOUBLE_READER);
		registerStandardWriter(Double.class, DOUBLE_WRITER);
		
		registerStandardReader(boolean.class, BOOLEAN_READER);
		registerStandardWriter(boolean.class, BOOLEAN_WRITER);
		
		registerStandardReader(Boolean.class, BOOLEAN_READER);
		registerStandardWriter(Boolean.class, BOOLEAN_WRITER);
		
		registerStandardReader(char.class, CHAR_READER);
		registerStandardWriter(char.class, CHAR_WRITER);
		
		registerStandardReader(Character.class, CHAR_READER);
		registerStandardWriter(Character.class, CHAR_WRITER);
		
		registerStandardReader(Date.class, DATE_READER);
		registerStandardWriter(Date.class, DATE_WRITER);
		
		registerStandardReader(Timestamp.class, TIMESTAMP_READER);
		registerStandardWriter(Timestamp.class, TIMESTAMP_WRITER);
		
		registerStandardReader(LocalDate.class, LOCAL_DATE_READER);
		registerStandardWriter(LocalDate.class, LOCAL_DATE_WRITER);
		
		registerStandardReader(LocalDateTime.class, LOCAL_DATE_TIME_READER);
		registerStandardWriter(LocalDateTime.class, LOCAL_DATE_TIME_WRITER);
		
		registerStandardReader(LocalTime.class, LOCAL_TIME_READER);
		registerStandardWriter(LocalTime.class, LOCAL_TIME_WRITER);
		
		registerStandardReader(String.class, STRING_READER);
		registerStandardWriter(String.class, STRING_WRITER);
		
		registerStandardReader(Object.class, OBJECT_READER);
		registerStandardWriter(Object.class, OBJECT_WRITER);
	}

	public static <T> void registerStandardReader(Class<T> type, DbReader<T> reader) {
		READER_REGISTRY.put(type, reader);
	}
	
	public static <T> void registerStandardWriter(Class<T> type, DbWriter<T> writer) {
		WRITER_REGISTRY.put(type, writer);
	}
	
	private Map<Class<?>, DbReader<?>> readers;
	private Map<Class<?>, DbWriter<?>> writers;
	
	public ConversionRegistry() {
		readers = new HashMap<>(READER_REGISTRY);
		writers = new HashMap<>(WRITER_REGISTRY);
	}
	
	public <T> void registerReader(Class<T> type, DbReader<T> reader) {
		this.readers.put(type, reader);
	}
	
	public <T> void registerWriter(Class<T> type, DbWriter<T> writer) {
		this.writers.put(type, writer);
	}
	
	@SuppressWarnings("unchecked")
	public <T> DbReader<T> getReader(Class<T> type) {
		
		DbReader<T> reader = null;
		
		if (readers.containsKey(type)) {
			reader = (DbReader<T>) readers.get(type);
		}
		else if (Enum.class.isAssignableFrom(type)) {
			
			try {
				Method valueOf = type.getMethod("valueOf", String.class);
				valueOf.setAccessible(true);
				
				reader = (rs, column) -> {
					try {
						return (T) valueOf.invoke(null, rs.getString(column));
					} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						throw new SQLException(e);
					}
				};
				
				registerStandardReader(type, reader); // Register so we can use this again for the enum type
				readers.put(type, reader);
				
			} catch (NoSuchMethodException | SecurityException e) {
				throw new RuntimeException(e); // Shouldn't happen if enum type
			}
		}
	
		if (reader == null) {
			throw new ConversionException("No suitable reader found for type " + type.getName());
		}
		
		return reader;
	}
	
	@SuppressWarnings("unchecked")
	public <T> DbWriter<T> getWriter(Class<T> type) {
		
		DbWriter<T> writer = null;
		
		if (writers.containsKey(type)) {
			writer = (DbWriter<T>) writers.get(type);
		}
		else if (Enum.class.isAssignableFrom(type)) {
			
			try {
				Method name = type.getMethod("name");
				name.setAccessible(true);
				
				writer = (ps, i, o) -> {
					try {
						ps.setString(i, name.invoke(o) + "");
					} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						throw new SQLException(e);
					}
				};
				
				registerStandardWriter(type, writer); // Register so we can use it for next time we write this enum type
				writers.put(type, writer);
				
			} catch (NoSuchMethodException | SecurityException e) {
				throw new RuntimeException(e); // Shouldn't happen if enum type
			}
		}
	
		if (writer == null) {
			throw new ConversionException("No suitable writer found for type " + type.getName());
		}
		
		return writer;
	}
	
}
