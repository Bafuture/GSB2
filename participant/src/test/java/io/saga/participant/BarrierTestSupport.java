package io.saga.participant;

import java.util.concurrent.atomic.AtomicInteger;
import org.h2.jdbcx.JdbcDataSource;

final class BarrierTestSupport {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private BarrierTestSupport() {
    }

    static Barrier newBarrier() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:barrier-" + COUNTER.incrementAndGet() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        return new Barrier(new BarrierStore(ds));
    }
}
