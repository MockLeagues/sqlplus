package com.tyler.sqlplus.conversion;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.tyler.sqlplus.exception.ConversionException;

public class ConversionRegistry {

	private static final Map<Class<?>, DbReader<?>> READER_REGISTRY = new HashMap<>();
	private static final Map<Class<?>, DbWriter<?>> WRITER_REGISTRY = new HashMap<>();
	
	static {
		
		registerStandardReader(int.class, (rs, col) -> rs.getInt(col));
		registerStandardWriter(int.class, (ps, i, o) -> ps.setInt(i, o));
		
		registerStandardReader(Integer.class, (rs, col) -> {
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
		
		registerStandardReader(short.class, (rs, col) -> rs.getShort(col));
		registerStandardWriter(short.class, (ps, i, o) -> ps.setShort(i, o));
		
		registerStandardReader(Short.class, (rs, col) -> {
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
		
		registerStandardReader(long.class, (rs, col) -> rs.getLong(col));
		registerStandardWriter(long.class, (ps, i, o) -> ps.setLong(i, o));
		
		registerStandardReader(Long.class, (rs, col) -> {
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
		
		registerStandardReader(float.class, (rs, col) -> rs.getFloat(col));
		registerStandardWriter(float.class, (ps, i, o) -> ps.setFloat(i, o));
		
		registerStandardReader(Float.class, (rs, col) -> {
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

		registerStandardReader(double.class, (rs, col) -> rs.getDouble(col));
		registerStandardWriter(double.class, (ps, i, o) -> ps.setDouble(i, o));
		
		registerStandardReader(Double.class, (rs, col) -> {
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
		
		registerStandardReader(boolean.class, (rs, col) -> rs.getBoolean(col));
		registerStandardWriter(boolean.class, (ps, i, o) -> ps.setBoolean(i, o));
		
		registerStandardReader(Boolean.class, (rs, col) -> {
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
		
		registerStandardReader(char.class, (rs, col) -> {
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
		
		registerStandardReader(Character.class, (rs, col) -> {
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
		
		registerStandardReader(String.class, (rs, col) -> rs.getString(col));
		registerStandardWriter(String.class, (ps, i, o) -> {
			if (o == null) {
				ps.setNull(i, Types.VARCHAR);
			}
			else {
				ps.setString(i, o);
			}
		});
		
		registerStandardReader(Date.class, (rs, col)  -> {
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
		
		registerStandardReader(Timestamp.class, (rs, column) -> {
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
		
		registerStandardReader(LocalDate.class, (rs, column) -> {
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
		
		registerStandardReader(LocalDateTime.class, (rs, column) -> {
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
		
		registerStandardReader(LocalTime.class, (rs, column) -> {
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
		
		registerStandardReader(Object.class, (rs, col) -> rs.getObject(col));
		registerStandardWriter(Object.class, (ps, i, o) -> ps.setObject(i, o));
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
