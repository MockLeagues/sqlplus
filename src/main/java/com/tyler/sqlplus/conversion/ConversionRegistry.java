package com.tyler.sqlplus.conversion;

import com.tyler.sqlplus.annotation.Conversion;
import com.tyler.sqlplus.exception.ConversionException;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ConversionRegistry {

	private static final Map<String, SQLConverter> DEFAULT_REGISTRY = new LinkedHashMap<>();
	static {

		registerDefaultConverter(byte.class, new SQLConverter<Byte>() {

			@Override
			public Class<Byte> getConvertedClass() {
				return byte.class;
			}

			@Override
			public Byte read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				return rs.getByte(column);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Byte byteVal) throws SQLException {
				ps.setByte(parameterIndex, byteVal);
			}

		});

		registerDefaultConverter(Byte.class, new SQLConverter<Byte>() {

			@Override
			public Class<Byte> getConvertedClass() {
				return Byte.class;
			}

			@Override
			public Byte read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				byte byteVal = rs.getByte(column);
				return rs.wasNull() ? null : byteVal;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Byte byteVal) throws SQLException {
				if (byteVal == null) {
					ps.setNull(parameterIndex, Types.TINYINT);
				} else {
					ps.setByte(parameterIndex, byteVal);
				}
			}

		});

		registerDefaultConverter(Integer.class, new SQLConverter<Integer>() {

			@Override
			public Class<Integer> getConvertedClass() {
				return Integer.class;
			}

			@Override
			public Integer read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				int obj = rs.getInt(column);
				return rs.wasNull() ? null : obj;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Integer integer) throws SQLException {
				if (integer == null) {
					ps.setNull(parameterIndex, Types.INTEGER);
				} else {
					ps.setInt(parameterIndex, integer);
				}
			}

		});

		registerDefaultConverter(int.class, new SQLConverter<Integer>() {

			@Override
			public Class<Integer> getConvertedClass() {
				return int.class;
			}

			@Override
			public Integer read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				return rs.getInt(column);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Integer integer) throws SQLException {
				ps.setInt(parameterIndex, integer);
			}

		});

		registerDefaultConverter(Integer.class, new SQLConverter<Integer>() {

			@Override
			public Class<Integer> getConvertedClass() {
				return Integer.class;
			}

			@Override
			public Integer read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				int obj = rs.getInt(column);
				return rs.wasNull() ? null : obj;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Integer integer) throws SQLException {
				if (integer == null) {
					ps.setNull(parameterIndex, Types.INTEGER);
				} else {
					ps.setInt(parameterIndex, integer);
				}
			}

		});

		registerDefaultConverter(short.class, new SQLConverter<Short>() {

			@Override
			public Class<Short> getConvertedClass() {
				return short.class;
			}

			@Override
			public Short read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				return rs.getShort(column);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Short shortVal) throws SQLException {
				ps.setShort(parameterIndex, shortVal);
			}

		});

		registerDefaultConverter(Short.class, new SQLConverter<Short>() {

			@Override
			public Class<Short> getConvertedClass() {
				return Short.class;
			}

			@Override
			public Short read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				short obj = rs.getShort(column);
				return rs.wasNull() ? null : obj;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Short shortVal) throws SQLException {
				if (shortVal == null) {
					ps.setNull(parameterIndex, Types.INTEGER);
				} else {
					ps.setShort(parameterIndex, shortVal);
				}
			}

		});

		registerDefaultConverter(long.class, new SQLConverter<Long>() {

			@Override
			public Class<Long> getConvertedClass() {
				return long.class;
			}

			@Override
			public Long read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				return rs.getLong(column);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Long longVal) throws SQLException {
				ps.setLong(parameterIndex, longVal);
			}

		});

		registerDefaultConverter(Long.class, new SQLConverter<Long>() {

			@Override
			public Class<Long> getConvertedClass() {
				return Long.class;
			}

			@Override
			public Long read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				long obj = rs.getLong(column);
				return rs.wasNull() ? null : obj;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Long longVal) throws SQLException {
				if (longVal == null) {
					ps.setNull(parameterIndex, Types.INTEGER);
				} else {
					ps.setLong(parameterIndex, longVal);
				}
			}

		});

		registerDefaultConverter(float.class, new SQLConverter<Float>() {

			@Override
			public Class<Float> getConvertedClass() {
				return float.class;
			}

			@Override
			public Float read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				return rs.getFloat(column);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Float floatVal) throws SQLException {
				ps.setFloat(parameterIndex, floatVal);
			}

		});

		registerDefaultConverter(Float.class, new SQLConverter<Float>() {

			@Override
			public Class<Float> getConvertedClass() {
				return Float.class;
			}

			@Override
			public Float read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				float obj = rs.getFloat(column);
				return rs.wasNull() ? null : obj;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Float floatVal) throws SQLException {
				if (floatVal == null) {
					ps.setNull(parameterIndex, Types.FLOAT);
				} else {
					ps.setFloat(parameterIndex, floatVal);
				}
			}

		});

		registerDefaultConverter(double.class, new SQLConverter<Double>() {

			@Override
			public Class<Double> getConvertedClass() {
				return double.class;
			}

			@Override
			public Double read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				return rs.getDouble(column);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Double doubleVal) throws SQLException {
				ps.setDouble(parameterIndex, doubleVal);
			}

		});

		registerDefaultConverter(Double.class, new SQLConverter<Double>() {

			@Override
			public Class<Double> getConvertedClass() {
				return Double.class;
			}

			@Override
			public Double read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				double obj = rs.getDouble(column);
				return rs.wasNull() ? null : obj;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Double doubleVal) throws SQLException {
				if (doubleVal == null) {
					ps.setNull(parameterIndex, Types.DOUBLE);
				} else {
					ps.setDouble(parameterIndex, doubleVal);
				}
			}

		});

		registerDefaultConverter(boolean.class, new SQLConverter<Boolean>() {

			@Override
			public Class<Boolean> getConvertedClass() {
				return boolean.class;
			}

			@Override
			public Boolean read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				return rs.getBoolean(column);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Boolean boolVal) throws SQLException {
				ps.setBoolean(parameterIndex, boolVal);
			}

		});

		registerDefaultConverter(Boolean.class, new SQLConverter<Boolean>() {

			@Override
			public Class<Boolean> getConvertedClass() {
				return Boolean.class;
			}

			@Override
			public Boolean read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				boolean obj = rs.getBoolean(column);
				return rs.wasNull() ? null : obj;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Boolean boolVal) throws SQLException {
				if (boolVal == null) {
					ps.setNull(parameterIndex, Types.BOOLEAN);
				} else {
					ps.setBoolean(parameterIndex, boolVal);
				}
			}

		});

		registerDefaultConverter("yes_no", new SQLConverter<Boolean>() {

			@Override
			public Class<Boolean> getConvertedClass() {
				return Boolean.class;
			}

			@Override
			public Boolean read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				String str = rs.getString(column);
				if (rs.wasNull()) {
					return targetType == Boolean.class ? null : false;
				} else {
					if (str.length() > 1) {
						throw new ConversionException("Cannot read 'Y' / 'N' boolean value from '" + str + "': length is greater than 1 character");
					}
					return str.charAt(0) == 'Y';
				}
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Boolean boolVal) throws SQLException {
				if (boolVal == null) {
					ps.setNull(parameterIndex, Types.BOOLEAN);
				} else {
					ps.setString(parameterIndex, boolVal ? "Y" : "N");
				}
			}

		});

		registerDefaultConverter(char.class, new SQLConverter<Character>() {

			@Override
			public Class<Character> getConvertedClass() {
				return char.class;
			}

			@Override
			public Character read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				String str = rs.getString(column);
				if (rs.wasNull()) {
					return Character.MIN_VALUE;
				}
				else {
					if (str.length() > 1) {
						throw new ConversionException("Failed to read char value from string '" + str + "'; length is greater than 1 character");
					}
					return str.charAt(0);
				}
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Character val) throws SQLException {
				ps.setString(parameterIndex, String.valueOf(val));
			}

		});

		registerDefaultConverter(Character.class, new SQLConverter<Character>() {

			@Override
			public Class<Character> getConvertedClass() {
				return Character.class;
			}

			@Override
			public Character read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				String str = rs.getString(column);
				if (rs.wasNull()) {
					return null;
				}
				else {
					if (str.length() > 1) {
						throw new ConversionException("Failed to read Character value from string '" + str + "'; length is greater than 1 character");
					}
					return str.charAt(0);
				}
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Character val) throws SQLException {
				if (val == null) {
					ps.setNull(parameterIndex, Types.CHAR);
				}
				else {
					ps.setString(parameterIndex, String.valueOf(val));
				}
			}

		});

		registerDefaultConverter(String.class, new SQLConverter<String>() {

			@Override
			public Class<String> getConvertedClass() {
				return String.class;
			}

			@Override
			public String read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				String str = rs.getString(column);
				if (rs.wasNull()) {
					return null;
				} else {
					return str;
				}
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, String val) throws SQLException {
				if (val == null) {
					ps.setNull(parameterIndex, Types.VARCHAR);
				} else {
					ps.setString(parameterIndex, val);
				}
			}

		});

		registerDefaultConverter(BigInteger.class, new SQLConverter<BigInteger>() {

			@Override
			public Class<BigInteger> getConvertedClass() {
				return BigInteger.class;
			}

			@Override
			public BigInteger read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				BigDecimal obj = rs.getBigDecimal(column);
				return rs.wasNull() ? null : obj.toBigInteger();
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, BigInteger bigIntVal) throws SQLException {
				if (bigIntVal == null) {
					ps.setNull(parameterIndex, Types.BIGINT);
				} else {
					ps.setBigDecimal(parameterIndex, new BigDecimal(bigIntVal));
				}
			}

		});

		registerDefaultConverter(BigDecimal.class, new SQLConverter<BigDecimal>() {

			@Override
			public Class<BigDecimal> getConvertedClass() {
				return BigDecimal.class;
			}

			@Override
			public BigDecimal read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				BigDecimal obj = rs.getBigDecimal(column);
				return rs.wasNull() ? null : obj;
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, BigDecimal bigDecVal) throws SQLException {
				if (bigDecVal == null) {
					ps.setNull(parameterIndex, Types.DECIMAL);
				} else {
					ps.setBigDecimal(parameterIndex, bigDecVal);
				}
			}

		});

		registerDefaultConverter(LocalDate.class, new SQLConverter<LocalDate>() {

			@Override
			public Class<LocalDate> getConvertedClass() {
				return LocalDate.class;
			}

			@Override
			public LocalDate read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				String strVal = rs.getString(column);
				return rs.wasNull() ? null : LocalDate.parse(strVal);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, LocalDate val) throws SQLException {
				if (val == null) {
					ps.setNull(parameterIndex, Types.DATE);
				} else {
					ps.setString(parameterIndex, val.toString());
				}
			}

		});

		registerDefaultConverter(LocalTime.class, new SQLConverter<LocalTime>() {

			@Override
			public Class<LocalTime> getConvertedClass() {
				return LocalTime.class;
			}

			@Override
			public LocalTime read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				String strVal = rs.getString(column);
				return rs.wasNull() ? null : LocalTime.parse(strVal);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, LocalTime val) throws SQLException {
				if (val == null) {
					ps.setNull(parameterIndex, Types.TIME);
				} else {
					ps.setString(parameterIndex, DateTimeFormatter.ofPattern("HH:mm:ss").format(val));
				}
			}

		});

		registerDefaultConverter(LocalDateTime.class, new SQLConverter<LocalDateTime>() {

			@Override
			public Class<LocalDateTime> getConvertedClass() {
				return LocalDateTime.class;
			}

			@Override
			public LocalDateTime read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				Timestamp stamp = rs.getTimestamp(column);
				return rs.wasNull() ? null : stamp.toLocalDateTime();
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, LocalDateTime val) throws SQLException {
				if (val == null) {
					ps.setNull(parameterIndex, Types.TIMESTAMP);
				} else {
					ps.setString(parameterIndex, val.toString());
				}
			}

		});

		registerDefaultConverter(Enum.class, new SQLConverter<Enum>() {

			@Override
			public Class<Enum> getConvertedClass() {
				return Enum.class;
			}

			@Override
			public Enum read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				try {
					String columnVal = rs.getString(column);
					if (rs.wasNull()) {
						return null;
					}
					Method valueOf = targetType.getDeclaredMethod("valueOf", String.class);
					valueOf.setAccessible(true);
					return (Enum<?>) valueOf.invoke(null, columnVal);
				} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Enum val) throws SQLException {
				if (val == null) {
					ps.setNull(parameterIndex, Types.VARCHAR);
				} else {
					ps.setString(parameterIndex, val.name());
				}
			}

		});

		registerDefaultConverter(Object.class, new SQLConverter<Object>() {

			@Override
			public Class<Object> getConvertedClass() {
				return Object.class;
			}

			@Override
			public Object read(ResultSet rs, String column, Class<?> targetType) throws SQLException {
				return rs.getObject(column);
			}

			@Override
			public void write(PreparedStatement ps, int parameterIndex, Object val) throws SQLException {
				ps.setObject(parameterIndex, val);
			}

		});

	}

	private Map<String, SQLConverter> registry;

	public ConversionRegistry() {
		registry = new HashMap<>(DEFAULT_REGISTRY);
	}

	public static <T> void registerDefaultConverter(Class<T> type, SQLConverter<T> converter) {
		registerDefaultConverter(type.getName(), converter);
	}

	public static <T> void registerDefaultConverter(String name, SQLConverter<T> converter) {
		DEFAULT_REGISTRY.put(name, converter);
	}

	public boolean containsConverter(Class<?> type) {
		return registry.containsKey(type.getName());
	}

	public <T> void registerConverter(Class<T> type, SQLConverter<T> converter) {
		registry.put(type.getName(), converter);
	}

	public <T> void registerConverter(String name, SQLConverter<T> converter) {
		registry.put(name, converter);
	}

	public <T> SQLConverter<T> getConverter(Field field) {
		if (field.isAnnotationPresent(Conversion.class)) {
			return getConverter(field.getAnnotation(Conversion.class).value());
		} else {
			return getConverter((Class<T>) field.getType());
		}
	}

	@SuppressWarnings("unchecked")
	public <T> SQLConverter<T> getConverter(String name) {
		if (registry.containsKey(name)) {
			return registry.get(name);
		} else {
			throw new IllegalArgumentException("No converter is registered for name '" + name + "'");
		}
	}

	public <T> SQLConverter<T> getConverter(Class<T> type) {
		return (SQLConverter<T>) registry.computeIfAbsent(type.getName(), t -> {
			return registry.values()
			               .stream()
			               .filter(converter -> converter.getConvertedClass().isAssignableFrom(type))
			               .findFirst()
			               .orElseThrow(() -> new ConversionException("No suitable converter found for " + type));
		});
	}
	
}
