package com.tyler.sqlplus.mapping;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.tyler.sqlplus.annotation.Column;
import com.tyler.sqlplus.annotation.Key;
import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;

public class ClassMetaData {

	// Master cache of meta data for each type
	private static final Map<Class<?>, ClassMetaData> TYPE_META = new HashMap<>();
	
	private Optional<Field> keyField;
	
	// Maintains a mapping of mapped result set column names fields and vice-versa
	private Map<String, Field> columnName_field = new HashMap<>();
	private Map<Field, String> field_columnName = new HashMap<>();

	public Optional<String> getMappedColumnName(Field f) {
		return Optional.ofNullable(field_columnName.get(f));
	}
	
	public Optional<Field> getMappedField(String columnLabel) {
		return Optional.ofNullable(columnName_field.get(columnLabel));
	}
	
	public Optional<Field> getKeyField() {
		return keyField;
	}
	
	public static ClassMetaData getMetaData(Class<?> type) {
		
		if (TYPE_META.containsKey(type)) {
			return TYPE_META.get(type);
		}
		
		ClassMetaData meta = new ClassMetaData();
		
		for (Field field : type.getDeclaredFields()) {
			
			if (field.isAnnotationPresent(SingleRelation.class) || field.isAnnotationPresent(MultiRelation.class)) {
				continue; // We can't map an object relation as a single field; the related class will have its own meta data
			}
			
			Column columnAnnot = field.getDeclaredAnnotation(Column.class);
			String mappedColName = columnAnnot != null && columnAnnot.name().length() > 0 ? columnAnnot.name() : field.getName();
			
			meta.columnName_field.put(mappedColName, field);
			meta.field_columnName.put(field, mappedColName);
			
			if (field.isAnnotationPresent(Key.class)) {
				meta.keyField = Optional.of(field);
			}
		}
		
		if (meta.keyField == null) {
			meta.keyField = Optional.empty();
		}
		TYPE_META.put(type, meta);
		return meta;
	}
	
}
