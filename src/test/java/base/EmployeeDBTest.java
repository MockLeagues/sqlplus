package base;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.BeforeClass;

import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.utility.Tasks.Task;

public class EmployeeDBTest {

	protected static SQLPlus SQL_PLUS = new SQLPlus(EmployeeDBTest::getConnection);
	static {
		try {
			Class.forName("org.h2.Driver");
		} catch (ClassNotFoundException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	
	protected static Connection getConnection() {
		try {
			return DriverManager.getConnection("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1", "sa", "sa");
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	@BeforeClass
	public static void setupDB() throws Exception {

		try (Connection conn = getConnection()) {
			
			// Table structure
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
	}
	
	@After
	public void flushTables() throws Exception {
		try (Connection conn = getConnection()) {
			conn.createStatement().executeUpdate("delete from address where address_id is not null");
			conn.createStatement().executeUpdate("alter table address auto_increment = 1");
			conn.createStatement().executeUpdate("delete from meeting where meeting_id is not null");
			conn.createStatement().executeUpdate("alter table meeting auto_increment = 1");
			conn.createStatement().executeUpdate("delete from employee where employee_id is not null");
			conn.createStatement().executeUpdate("alter table employee auto_increment = 1");
			conn.createStatement().executeUpdate("delete from office where office_id is not null");
			conn.createStatement().executeUpdate("alter table office auto_increment = 1");
			conn.createStatement().executeUpdate("delete from product where product_id is not null");
			conn.createStatement().executeUpdate("alter table product auto_increment = 1");
			conn.createStatement().executeUpdate("delete from `order` where order_id is not null");
			conn.createStatement().executeUpdate("alter table `order` auto_increment = 1");
			conn.createStatement().executeUpdate("delete from order_product_map where order_product_id is not null");
			conn.createStatement().executeUpdate("alter table order_product_map auto_increment = 1");
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
