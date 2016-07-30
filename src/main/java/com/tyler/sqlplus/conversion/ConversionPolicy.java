package com.tyler.sqlplus.conversion;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for a series of attribute converters.
 * 
 * Custom attribute converters can be added to an instance of this class depending on if special serialization / deserialization is
 * needed for a particular java type. If a custom converter is not found, one of the defaults will be returned
 */
public class ConversionPolicy {

	private static final Map<Class<?>, AttributeConverter<?>> DEFAULT_CONVERTERS = new HashMap<>();
	
	public static <T> void setDefaultConverter(Class<T> type, AttributeConverter<T> converter) {
		DEFAULT_CONVERTERS.put(type, converter);
	}
	
	static {
		
		setDefaultConverter(Integer.class, new AttributeConverter<Integer>() {

			@Override
			public Integer get(ResultSet rs, int column) throws SQLException {
				return rs.getInt(column);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, Integer obj) throws SQLException {
				ps.setInt(parameterIndex, obj);
			}
			
		});
		
		setDefaultConverter(Short.class, new AttributeConverter<Short>() {

			@Override
			public Short get(ResultSet rs, int column) throws SQLException {
				return rs.getShort(column);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, Short obj) throws SQLException {
				ps.setShort(parameterIndex, obj);
			}
			
		});
		
		setDefaultConverter(Long.class, new AttributeConverter<Long>() {

			@Override
			public Long get(ResultSet rs, int column) throws SQLException {
				return rs.getLong(column);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, Long obj) throws SQLException {
				ps.setLong(parameterIndex, obj);
			}
			
		});
		
		setDefaultConverter(Float.class, new AttributeConverter<Float>() {

			@Override
			public Float get(ResultSet rs, int column) throws SQLException {
				return rs.getFloat(column);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, Float obj) throws SQLException {
				ps.setFloat(parameterIndex, obj);
			}
			
		});
		
		setDefaultConverter(Double.class, new AttributeConverter<Double>() {

			@Override
			public Double get(ResultSet rs, int column) throws SQLException {
				return rs.getDouble(column);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, Double obj) throws SQLException {
				ps.setDouble(parameterIndex, obj);
			}
			
		});
		
		setDefaultConverter(Character.class, new AttributeConverter<Character>() {

			@Override
			public Character get(ResultSet rs, int column) throws SQLException {
				String str = rs.getString(column);
				return str == null || str.isEmpty() ? null : str.charAt(0);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, Character obj) throws SQLException {
				ps.setString(parameterIndex, new String(new char[]{obj}));
			}
			
		});
		
		setDefaultConverter(LocalDate.class, new AttributeConverter<LocalDate>() {

			@Override
			public LocalDate get(ResultSet rs, int column) throws SQLException {
				return LocalDate.parse(rs.getString(column));
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, LocalDate obj) throws SQLException {
				ps.setString(parameterIndex, obj.toString());
			}
			
		});
		
		setDefaultConverter(String.class, new AttributeConverter<String>() {

			@Override
			public String get(ResultSet rs, int column) throws SQLException {
				return rs.getString(column);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, String obj) throws SQLException {
				ps.setString(parameterIndex, obj);
			}
			
		});
		
		setDefaultConverter(Boolean.class, new AttributeConverter<Boolean>() {

			@Override
			public Boolean get(ResultSet rs, int column) throws SQLException {
				return rs.getInt(column) == 1;
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, Boolean obj) throws SQLException {
				ps.setInt(parameterIndex, obj ? 1 : 0);
			}
			
		});
	
		setDefaultConverter(Object.class, new AttributeConverter<Object>() {

			@Override
			public Object get(ResultSet rs, int column) throws SQLException {
				return rs.getObject(column);
			}

			@Override
			public void set(PreparedStatement ps, int parameterIndex, Object obj) throws SQLException {
				ps.setObject(parameterIndex, obj);
			}
			
		});
		
	}
	
	private Map<Class<?>, AttributeConverter<?>> customConverters = new HashMap<>();
	
	public <T> void setConverter(Class<T> type, AttributeConverter<T> converter) {
		this.customConverters.put(type, converter);
	}
	
	@SuppressWarnings("unchecked")
	public <T> AttributeConverter<T> findConverter(Class<T> targetType) {
		
		AttributeConverter<T> converter = null;
		
		if (customConverters.containsKey(targetType)) {
			converter = (AttributeConverter<T>) customConverters.get(targetType);
		}
		else if (DEFAULT_CONVERTERS.containsKey(targetType)) {
			converter = (AttributeConverter<T>) DEFAULT_CONVERTERS.get(targetType);
		}
		else if (Enum.class.isAssignableFrom(targetType)) {
			
			try {
				
				Method valueOf = targetType.getMethod("valueOf", String.class);
				Method name = targetType.getMethod("name");
				
				AttributeConverter<T> enumConverter = new AttributeConverter<T>() {
					
					@Override
					public T get(ResultSet rs, int column) throws SQLException {
						try {
							return (T) valueOf.invoke(null, rs.getString(column));
						} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							throw new SQLException(e);
						}
					}
					
					@Override
					public void set(PreparedStatement ps, int parameterIndex, T obj) throws SQLException {
						try {
							ps.setString(parameterIndex, name.invoke(obj) + "");
						} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							throw new SQLException(e);
						}
					}
					
				};
				
				converter = enumConverter;
				setDefaultConverter(targetType, enumConverter);
			} catch (NoSuchMethodException | SecurityException e) {
				throw new RuntimeException(e); // Shouldn't happen if enum type
			}
		}
		else {
			converter = (AttributeConverter<T>) DEFAULT_CONVERTERS.get(Object.class);
		}
	
		return converter;
	}
	
}
