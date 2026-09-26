package com.example.saga.participant;

import org.h2.jdbcx.JdbcDataSource;

import java.util.UUID;

final class BarrierTestSupport {

    private BarrierTestSupport() {
    }

    static JdbcDataSource inMemoryDataSource() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        return ds;
    }
}
