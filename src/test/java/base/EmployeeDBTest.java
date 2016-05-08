package base;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.utility.Tasks.Task;

public class EmployeeDBTest {

	protected static SQLPlus SQL_PLUS = new SQLPlus(EmployeeDBTest::getConnection);
	
	protected static Connection getConnection() {
		try {
			return DriverManager.getConnection("jdbc:mysql://localhost:3306/tester", "tester", "tester");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@BeforeClass
	public static void setupDB() throws Exception {

		try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysql", "tester", "tester")) {
			
			// Create the database
			conn.createStatement().executeUpdate("CREATE DATABASE tester");
			conn.createStatement().executeUpdate("USE tester");
			
			// Table structure
			conn.createStatement().executeUpdate(
				"CREATE TABLE `address` (" +
					"`address_id` int(11) NOT NULL AUTO_INCREMENT," +
					"`street` varchar(45) DEFAULT NULL," +
					"`city` varchar(45) DEFAULT NULL," +
					"`state` varchar(45) DEFAULT NULL," +
					"`zip` varchar(45) DEFAULT NULL," +
					"PRIMARY KEY (`address_id`)" +
				")"
			);
			
			conn.createStatement().executeUpdate(
				"CREATE TABLE `meeting` (" +
					"`meeting_id` int(11) NOT NULL AUTO_INCREMENT," +
					"`office_id` int(11) NOT NULL," +
					"`start_time` int(11) NOT NULL," +
					"`end_time` int(11) NOT NULL," +
					"`topic` varchar(45) NOT NULL," +
					"PRIMARY KEY (`meeting_id`)" +
				")"
			);
			
			conn.createStatement().executeUpdate(
				"CREATE TABLE `employee` (" +
					"`employee_id` int(11) NOT NULL AUTO_INCREMENT," +
					"`type` varchar(45) NOT NULL," +
					"`name` varchar(45) NOT NULL," +
					"`salary` int(11) NOT NULL," +
					"`hired` date NOT NULL," +
					"`address_id` int(11) DEFAULT NULL," +
					"PRIMARY KEY (`employee_id`)" +
				")"
			);
			
			conn.createStatement().executeUpdate(
				"CREATE TABLE `office` (" +
					"`office_id` int(11) NOT NULL AUTO_INCREMENT," +
					"`office_name` varchar(45) NOT NULL," +
					"`primary` tinyint(4) NOT NULL," +
					"`employee_id` int(11) DEFAULT NULL," +
					"PRIMARY KEY (`office_id`)" +
				")"
			);
			
			conn.createStatement().executeUpdate(
				"CREATE TABLE `order` (" +
					"`order_id` int(11) NOT NULL AUTO_INCREMENT," +
					"`date_submitted` date NULL," +
					"`submitter` varchar(45) NULL," +
					"PRIMARY KEY (`order_id`)" +
				")"
			);
			
			conn.createStatement().executeUpdate(
				"CREATE TABLE `product` (" +
					"`product_id` int(11) NOT NULL AUTO_INCREMENT," +
					"`price` decimal(10,2) NOT NULL," +
					"`uom` varchar(45) NULL," +
					"PRIMARY KEY (`product_id`)" +
				")"
			);
			
			conn.createStatement().executeUpdate(
				"CREATE TABLE `order_product_map` (" +
					"`order_product_id` int(11) NOT NULL AUTO_INCREMENT," +
					"`order_id` int(11) NOT NULL," +
					"`product_id` int(11) NOT NULL," +
					"PRIMARY KEY (`order_product_id`)" +
				")"
			);
		}
	}
	
	@After
	public void flushTables() throws Exception {
		try (Connection conn = getConnection()) {
			conn.createStatement().executeUpdate("delete from address where address_id is not null");
			conn.createStatement().executeUpdate("alter table address AUTO_INCREMENT = 1");
			
			conn.createStatement().executeUpdate("delete from meeting where meeting_id is not null");
			conn.createStatement().executeUpdate("alter table meeting AUTO_INCREMENT = 1");
			
			conn.createStatement().executeUpdate("delete from employee where employee_id is not null");
			conn.createStatement().executeUpdate("alter table employee AUTO_INCREMENT = 1");
			
			conn.createStatement().executeUpdate("delete from office where office_id is not null");
			conn.createStatement().executeUpdate("alter table office AUTO_INCREMENT = 1");
			
			conn.createStatement().executeUpdate("delete from product where product_id is not null");
			conn.createStatement().executeUpdate("alter table product AUTO_INCREMENT = 1");
			
			conn.createStatement().executeUpdate("delete from `order` where order_id is not null");
			conn.createStatement().executeUpdate("alter table `order` AUTO_INCREMENT = 1");
			
			conn.createStatement().executeUpdate("delete from order_product_map where order_product_id is not null");
			conn.createStatement().executeUpdate("alter table order_product_map AUTO_INCREMENT = 1");
		}
	}
	
	@AfterClass
	public static void deleteDB() throws Exception {
		try (Connection conn = getConnection()) {
			conn.createStatement().executeUpdate("DROP DATABASE tester");
			conn.close();
		}
	}
	
	protected static String[][] query(String sql) {
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
	
	protected static void transact(String... sql) throws Exception {
		try (Connection conn = getConnection()) {
			for (String sq : sql) 
				conn.createStatement().executeUpdate(sq);
		}
	}
	
	protected static void assertThrows(Task t, Class<? extends Throwable> expectType) {
		try {
			t.run();
			fail("Expected test to throw instance of " + expectType.getName() + " but no error was thrown");
		}
		catch (Throwable thrownError) {
			if (!expectType.equals(thrownError.getClass())) {
				fail("Expected test to throw instance of " + expectType.getName() + " but no instead got error of type " + thrownError.getClass().getName());
			}
		}
	}
	
}
