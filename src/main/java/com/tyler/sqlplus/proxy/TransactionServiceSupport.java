package com.tyler.sqlplus.proxy;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.annotation.SqlPlusInject;

/**
 * Base support class for classes which contain @Transactional methods
 */
public class TransactionServiceSupport {

	@SqlPlusInject
	private SQLPlus sqlPlus;

	protected Session session() {
		return sqlPlus.getCurrentSession();
	}

}
