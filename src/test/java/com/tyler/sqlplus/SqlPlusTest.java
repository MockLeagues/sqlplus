package com.tyler.sqlplus;

import static org.junit.Assert.*;
import static com.tyler.sqlplus.test.SqlPlusTesting.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Rule;
import org.junit.Test;

import com.tyler.sqlplus.annotation.ServiceSession;
import com.tyler.sqlplus.annotation.Transactional;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.rule.H2EmployeeDBRule;
import com.tyler.sqlplus.rule.H2EmployeeDBRule.Address;

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
		
		@ServiceSession
		private Session session;
		
		@Transactional
		public List<Address> getAddresses() {
			return session.createQuery("select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" from address")
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
		@ServiceSession private String notASession;
	}
	
	@Test
	public void testTransactionAwareServiceThrowsErrorIfSessionInjectFieldTypeIsNotSession() throws Exception {
		assertThrows(
			() -> h2.getSQLPlus().createTransactionAwareService(TransactionAwareServiceBadField.class),
			ReflectionException.class,
			"@ServiceSession annotated field " + TransactionAwareServiceBadField.class.getDeclaredField("notASession") + " must be a Session type"
		);
	}
	
}
