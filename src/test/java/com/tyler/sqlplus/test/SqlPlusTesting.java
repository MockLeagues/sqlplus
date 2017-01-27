package com.tyler.sqlplus.test;

import com.tyler.sqlplus.SqlPlus;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.function.Task;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SqlPlusTesting {

	public static void assertThrows(Task t, Class<? extends Throwable> expectType) {
		assertThrows(t, expectType, null);
	}
	
	public static void assertThrows(Task t, Class<? extends Throwable> expectType, String expectMsg) {
		try {
			t.run();
			fail("Expected test to throw instance of " + expectType.getName() + " but no error was thrown");
		}
		catch (Throwable thrownError) {
			if (thrownError.getClass() == AssertionError.class) {
				throw new RuntimeException(thrownError);
			}
			if (!expectType.equals(thrownError.getClass())) {
				fail("Expected test to throw instance of " + expectType.getName() + " but no instead got error of type " + thrownError.getClass().getName());
			}
			if (expectMsg != null) {
				assertEquals(expectMsg, thrownError.getMessage());
			}
		}
	}

	static class UserDefinedMenu {

		public String id;
		public String menuTitle;
		public String menuId;

		@LoadQuery("select * from USER_DEFINED_MENU_ITEM where USER_DEFINED_MENU_ID=:id")
		Collection<UserDefinedMenuItem> items = new ArrayList<>();

		public Collection<UserDefinedMenuItem> getItems() {
			return items;
		}

		@Override
		public String toString() {
			return "UserDefinedMenu{" +
							"id='" + id + '\'' +
							", menuTitle='" + menuTitle + '\'' +
							", menuId='" + menuId + '\'' +
							'}';
		}
	}

	public static class UserDefinedMenuItem {
		public String id;
		public String userDefinedMenuId;
		public String menuItemText;
		public String url;

		@Override
		public String toString() {
			return "UserDefinedMenuItem{" +
							"id='" + id + '\'' +
							", userDefinedMenuId='" + userDefinedMenuId + '\'' +
							", menuItemText='" + menuItemText + '\'' +
							", url='" + url + '\'' +
							'}';
		}
	}

	public static void main(String[] args) throws Exception {

		Class.forName(net.sourceforge.jtds.jdbc.Driver.class.getName());

		SqlPlus sqlServer = new SqlPlus(() -> {
			try {
				return DriverManager.getConnection("jdbc:jtds:sqlserver://ums1devsdbws03v/IL_Tyler1_Prime;applicationName=InfoLease10;instance=DIL1");
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});

		sqlServer.transact(sess -> {

			List<UserDefinedMenu> menus = sess.createQuery("select * from USER_DEFINED_MENU").fetchAs(UserDefinedMenu.class);

			for (UserDefinedMenu m : menus) {
				System.out.println(m.getItems());
			}

		});

	}

}
