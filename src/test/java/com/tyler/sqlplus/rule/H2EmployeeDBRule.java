package com.tyler.sqlplus.rule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.tyler.sqlplus.exception.SQLRuntimeException;

public class H2EmployeeDBRule extends AbstractDBRule {

	static {
		try {
			Class.forName(org.h2.Driver.class.getName());
		} catch (ClassNotFoundException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	
	@Override
	public Connection getConnection() {
		try {
			return DriverManager.getConnection("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1", "sa", "sa");
		} catch (SQLException e) {
			throw new SQLRuntimeException(e);
		}
	}
	
	@Override
	public void setupSchema() {
		
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
						"`salary` int(11) null," +
						"`hired` date null," +
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
	
	@Override
	public void destroySchema() {
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
