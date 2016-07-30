package base;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.ExternalResource;

import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.exception.SQLRuntimeException;

public class EmployeeDBRule extends ExternalResource {

	static {
		try {
			Class.forName(org.h2.Driver.class.getName());
		} catch (ClassNotFoundException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	
	private SQLPlus sqlPlus;
	
	public EmployeeDBRule() {
		this.sqlPlus = new SQLPlus(this::getConnection);
	}
	
	public SQLPlus getSQLPlus() {
		return sqlPlus;
	}
	
	public Connection getConnection() {
		try {
			return DriverManager.getConnection("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1", "sa", "sa");
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	@Override
	public void before() {
		
		try {
			
			try (Connection conn = getConnection()) {
				
				conn.createStatement().executeUpdate(
					"create table `address` (" +
						"`address_id` int(11) not null auto_increment," +
						"`street` varchar(45) default null," +
						"`city` varchar(45) default null," +
						"`state` varchar(45) default null," +
						"`zip` varchar(45) default null," +
						"primary key (`address_id`)" +
					")"
				);
				
				conn.createStatement().executeUpdate(
					"create table `meeting` (" +
						"`meeting_id` int(11) not null auto_increment," +
						"`office_id` int(11) not null," +
						"`start_time` int(11) not null," +
						"`end_time` int(11) not null," +
						"`topic` varchar(45) not null," +
						"primary key (`meeting_id`)" +
					")"
				);
				
				conn.createStatement().executeUpdate(
					"create table `employee` (" +
						"`employee_id` int(11) not null auto_increment," +
						"`type` varchar(45) not null," +
						"`name` varchar(45) not null," +
						"`salary` int(11) not null," +
						"`hired` date not null," +
						"`address_id` int(11) default null," +
						"primary key (`employee_id`)" +
					")"
				);
				
				conn.createStatement().executeUpdate(
					"create table `office` (" +
						"`office_id` int(11) not null auto_increment," +
						"`office_name` varchar(45) not null," +
						"`primary` tinyint(4) not null," +
						"`employee_id` int(11) default null," +
						"primary key (`office_id`)" +
					")"
				);
				
				conn.createStatement().executeUpdate(
					"create table `order` (" +
						"`order_id` int(11) not null auto_increment," +
						"`date_submitted` date null," +
						"`submitter` varchar(45) null," +
						"primary key (`order_id`)" +
					")"
				);
				
				conn.createStatement().executeUpdate(
					"create table `product` (" +
						"`product_id` int(11) not null auto_increment," +
						"`price` decimal(10,2) not null," +
						"`uom` varchar(45) null," +
						"primary key (`product_id`)" +
					")"
				);
				
				conn.createStatement().executeUpdate(
					"create table `order_product_map` (" +
						"`order_product_id` int(11) not null auto_increment," +
						"`order_id` int(11) not null," +
						"`product_id` int(11) not null," +
						"primary key (`order_product_id`)" +
					")"
				);
			}
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
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
	
	public void transact(String... cmds) throws SQLException {
		try (Connection conn = getConnection()) {
			Statement st = conn.createStatement();
			for (String cmd : cmds) {
				st.addBatch(cmd);
			}
			st.executeBatch();
		}
	}
	
	@Override
	public void after() {
		try {
			try (Connection conn = getConnection()) {
				conn.createStatement().executeUpdate("drop table address");
				conn.createStatement().executeUpdate("drop table meeting");
				conn.createStatement().executeUpdate("drop table employee");
				conn.createStatement().executeUpdate("drop table office");
				conn.createStatement().executeUpdate("drop table product");
				conn.createStatement().executeUpdate("drop table `order`");
				conn.createStatement().executeUpdate("drop table order_product_map");
			}
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
}
