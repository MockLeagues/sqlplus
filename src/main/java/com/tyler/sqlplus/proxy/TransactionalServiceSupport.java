package com.tyler.sqlplus.proxy;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.annotation.Database;

/**
 * Base support class for classes which contain @Transactional methods
 */
public class TransactionalServiceSupport {

	@Database
	private SQLPlus sqlPlus;

	protected Session session() {
		return sqlPlus.getCurrentSession();
	}

}
