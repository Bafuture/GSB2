package com.example.saga.core;

import org.h2.jdbcx.JdbcDataSource;

import java.util.UUID;

final class CoreTestSupport {

    private CoreTestSupport() {
    }

    static JdbcDataSource inMemoryDataSource() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        return ds;
    }

    static RetryPolicy fastRetry(int maxAttempts) {
        return new RetryPolicy(maxAttempts, 5, 2.0);
    }
}
