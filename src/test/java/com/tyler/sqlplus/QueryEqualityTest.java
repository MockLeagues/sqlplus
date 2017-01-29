package com.tyler.sqlplus;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class QueryEqualityTest {

	@Test
	public void equalityWithNoParams() throws Exception {

		assertEquals(
						new Query("select * from blah", null),
						new Query("select * from blah", null)
		);

		assertNotEquals(
						new Query("select * from A", null),
						new Query("select * from B", null)
		);

	}

	@Test
	public void equalityWithQuestionMarkParams() throws Exception {

		assertEquals(
						new Query("select * from blah where col = ?", null).setParameter(1, "param"),
						new Query("select * from blah where col = ?", null).setParameter(1, "param")
		);

		assertEquals(
						new Query("select * from blah where col1 = ? and col2 = ?", null).setParameter(1, "param1").setParameter(2, "param2"),
						new Query("select * from blah where col1 = ? and col2 = ?", null).setParameter(1, "param1").setParameter(2, "param2")
		);

		// Same SQL but different params
		assertNotEquals(
						new Query("select * from blah where col = ?", null).setParameter(1, "paramA"),
						new Query("select * from blah where col = ?", null).setParameter(1, "paramB")
		);

		// Same params but different SQL
		assertNotEquals(
						new Query("select * from A where col = ?", null).setParameter(1, "param"),
						new Query("select * from B where col = ?", null).setParameter(1, "param")
		);

	}

	@Test
	public void equalityWithLabelParams() throws Exception {

		assertEquals(
						new Query("select * from blah where col = :col", null).setParameter("col", "param"),
						new Query("select * from blah where col = :col", null).setParameter("col", "param")
		);

		assertEquals(
						new Query("select * from blah where col1 = :col1 and col2 = :col2", null).setParameter("col1", "param1").setParameter("col2", "param2"),
						new Query("select * from blah where col1 = :col1 and col2 = :col2", null).setParameter("col1", "param1").setParameter("col2", "param2")
		);

		// Same SQL but different params
		assertNotEquals(
						new Query("select * from blah where col = :col", null).setParameter("col", "paramA"),
						new Query("select * from blah where col = :col", null).setParameter("col", "paramB")
		);

		// Same params but different SQL
		assertNotEquals(
						new Query("select * from A where col = :col", null).setParameter("col", "param"),
						new Query("select * from B where col = :col", null).setParameter("col", "param")
		);

	}

	@Test
	public void hashCodeWithNoParams() throws Exception {

		assertEquals(
						new Query("select * from blah", null).hashCode(),
						new Query("select * from blah", null).hashCode()
		);

		assertNotEquals(
						new Query("select * from A", null).hashCode(),
						new Query("select * from B", null).hashCode()
		);

	}

	@Test
	public void hashCodeWithParams() throws Exception {

		assertEquals(
						new Query("select * from blah where col = ?", null).setParameter(1, "param").hashCode(),
						new Query("select * from blah where col = ?", null).setParameter(1, "param").hashCode()
		);

		// Same SQL but different params
		assertNotEquals(
						new Query("select * from blah where col = ?", null).setParameter(1, "paramA").hashCode(),
						new Query("select * from blah where col = ?", null).setParameter(1, "paramB").hashCode()
		);

		// Same params but different SQL
		assertNotEquals(
						new Query("select * from A where col = ?", null).setParameter(1, "param").hashCode(),
						new Query("select * from B where col = ?", null).setParameter(1, "param").hashCode()
		);

	}

}
