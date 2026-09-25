package io.saga.coordinator;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.jdbcx.JdbcDataSource;

final class TestDbs {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private TestDbs() {
    }

    /** Fresh in-memory H2 database, kept alive for the lifetime of the JVM. */
    static JdbcDataSource inMemory() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test-" + COUNTER.incrementAndGet() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        return ds;
    }

    /** File-based H2 database so a "restarted" process can reopen the same state. */
    static JdbcDataSource fileBased(Path directory) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:file:" + directory.resolve("saga-state").toAbsolutePath());
        ds.setUser("sa");
        return ds;
    }
}
