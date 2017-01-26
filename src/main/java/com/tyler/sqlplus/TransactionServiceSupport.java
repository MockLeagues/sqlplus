package com.tyler.sqlplus;

import com.tyler.sqlplus.annotation.SqlPlusInject;

/**
 * Base support class for classes which contain @Transactional methods
 */
public class TransactionServiceSupport {

	@SqlPlusInject
	private SqlPlus sqlPlus;

	protected Session session() {
		return sqlPlus.getCurrentSession();
	}

}
