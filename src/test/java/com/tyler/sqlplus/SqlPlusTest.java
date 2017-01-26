package com.tyler.sqlplus;

import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.annotation.Transactional;
import com.tyler.sqlplus.annotation.SqlPlusInject;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.rule.H2EmployeeDBRule;
import com.tyler.sqlplus.rule.H2EmployeeDBRule.Address;
import org.junit.Rule;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

import static com.tyler.sqlplus.test.SqlPlusTesting.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SqlPlusTest {

	@Rule
	public H2EmployeeDBRule h2 = new H2EmployeeDBRule();
	
	@Test
	public void testCurrentThreadCorrectlyPullsCurrentSession() throws Exception {
		
		List<Session> sessionsRetrieved = new ArrayList<>();
		
		Callable<Object> childCall = () -> {
			h2.getSQLPlus().transact(conn -> {
				sessionsRetrieved.add(conn);
			});
			return null;
		};
		
		Callable<Object> parentCall = () -> {
			h2.getSQLPlus().transact(sess -> {
				sessionsRetrieved.add(sess);
				childCall.call(); // When the child call opens it's session, the session it gets should be the exact same object
			});
			return null;
		};
		
		parentCall.call();
		assertTrue(sessionsRetrieved.get(0) == sessionsRetrieved.get(1));
	}
	
	static class TransactionAwareService {

		@SqlPlusInject
		private SqlPlus sqlplus;

		@Transactional
		public List<Address> getAddresses() {
			return sqlplus.getCurrentSession()
			              .createQuery("select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" from address")
			              .fetchAs(Address.class);
		}
		
	}
	
	@Test
	public void testTransactionAwareServicesExecuteMethodsInTransaction() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		TransactionAwareService service = h2.getSQLPlus().createTransactionAwareService(TransactionAwareService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals(3, addresses.size());
	}
	
	static class TransactionAwareServiceBadField {
		@SqlPlusInject
		private String notASession;
	}
	
	@Test
	public void testTransactionAwareServiceThrowsErrorIfInjectFieldTypeIsNotSqlPlus() throws Exception {
		assertThrows(
			() -> h2.getSQLPlus().createTransactionAwareService(TransactionAwareServiceBadField.class),
			ReflectionException.class,
			SqlPlusInject.class + " annotated field " + TransactionAwareServiceBadField.class.getDeclaredField("notASession") + " must be of type " + SqlPlus.class
		);
	}

	static class SupportedService extends TransactionServiceSupport {

		@Transactional
		public List<Address> getAddresses() {
			return session().createQuery("select * from address").fetchAs(Address.class);
		}

	}

	@Test
	public void testServiceClassExtendingTransactionServiceSupport() throws Exception {
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);

		SupportedService service = h2.getSQLPlus().createTransactionAwareService(SupportedService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals(3, addresses.size());
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

	static class UserDefinedMenuItem {
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
