package com.tyler.sqlplus.rule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.ExternalResource;

import com.tyler.sqlplus.Configuration;
import com.tyler.sqlplus.SqlPlus;

public abstract class AbstractDBRule extends ExternalResource {

	private SqlPlus sqlPlus;
	
	public AbstractDBRule() {
		this.sqlPlus = new SqlPlus(this::getConnection);
	}
	
	public SqlPlus getSQLPlus() {
		return sqlPlus;
	}
	
	public SqlPlus buildSqlPlus(Configuration config) {
		return new SqlPlus(config, this::getConnection);
	}
	
	public String[][] query(String sql) {
		try (Connection conn = getConnection()) {
			List<String[]> rows = new ArrayList<>();
			ResultSet rs = conn.createStatement().executeQuery(sql);
			int cols = rs.getMetaData().getColumnCount();
			while (rs.next()) {
				List<String> row = new ArrayList<>();
				for (int i = 1; i <= cols; i++) {
					row.add(rs.getString(i));
				}
				rows.add(row.toArray(new String[row.size()]));
			}
			return rows.toArray(new String[rows.size()][cols]);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void batch(String... cmds) throws SQLException {
		try (Connection conn = getConnection()) {
			Statement st = conn.createStatement();
			for (String cmd : cmds) {
				st.addBatch(cmd);
			}
			st.executeBatch();
		}
	}
	
	@Override
	public void before() {
		setupSchema();
	}
	
	@Override
	public void after() {
		destroySchema();
	}
	
	public abstract Connection getConnection();
	
	public abstract void setupSchema();
	
	public abstract void destroySchema();
	
}
