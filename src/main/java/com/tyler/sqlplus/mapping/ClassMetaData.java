package com.tyler.sqlplus.mapping;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.tyler.sqlplus.annotation.Column;
import com.tyler.sqlplus.annotation.Key;
import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;

public class ClassMetaData {

	// Master cache of meta data for each type
	private static final Map<Class<?>, ClassMetaData> TYPE_META = new HashMap<>();
	
	// Can remains null if the class does not have a key field
	private Field keyField;
	
	// Maintains a mapping of mapped result set column names fields and vice-versa
	private Map<String, Field> column_member = new HashMap<>();
	private Map<Field, String> member_column = new HashMap<>();

	public String getMappedColumnName(Field f) {
		return member_column.get(f);
	}
	
	public Field getMappedField(String columnLabel) {
		return column_member.get(columnLabel);
	}
	
	public Field getKeyField() {
		return keyField;
	}
	
	public static ClassMetaData getMetaData(Class<?> type) {
		
		if (TYPE_META.containsKey(type)) {
			return TYPE_META.get(type);
		}
		
		ClassMetaData meta = new ClassMetaData();
		
		for (Field field : type.getDeclaredFields()) {
			if (field.isAnnotationPresent(SingleRelation.class) || field.isAnnotationPresent(MultiRelation.class)) {
				continue;
			}
			Column annot = field.getDeclaredAnnotation(Column.class);
			String mappedColName = annot != null && annot.name().length() > 0 ? annot.name() : field.getName();
			meta.column_member.put(mappedColName, field);
			meta.member_column.put(field, mappedColName);
			if (field.isAnnotationPresent(Key.class)) {
				meta.keyField = field;
			}
		}
		
		TYPE_META.put(type, meta);
		return meta;
	}
	
}
