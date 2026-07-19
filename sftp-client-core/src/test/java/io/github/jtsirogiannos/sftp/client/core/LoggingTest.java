package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Logging is only useful if it says the right things and never the wrong ones.
 *
 * <p>The wrong ones matter more here than in most libraries: this client is handed passwords and key
 * passphrases, and a credential that reaches a log file has escaped every protection around it —
 * log aggregation ships it off the host, and it survives long after the process. So the central
 * assertion is a negative one, checked across an entire client lifecycle.
 *
 * <p>Captured through {@code slf4j-jdk14}, which is bound in test scope only. Nothing is bound at
 * runtime: picking a backend stays the application's decision, per ROADMAP Phase 1.
 */
public class LoggingTest {

    private static final String PASSWORD = "sup3r-s3cret-should-never-be-logged";
    private static final String PASSPHRASE = "k3y-passphrase-also-secret";

    private final List<LogRecord> records = new CopyOnWriteArrayList<>();
    private java.util.logging.Logger julRoot;
    private Handler handler;
    private Level previousLevel;

    @BeforeMethod
    public void captureLogs() {
        records.clear();
        handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                records.add(record);
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        handler.setLevel(Level.ALL);
        julRoot = java.util.logging.Logger.getLogger("io.github.jtsirogiannos");
        previousLevel = julRoot.getLevel();
        julRoot.setLevel(Level.ALL); // SLF4J debug maps to JUL FINE
        julRoot.addHandler(handler);
    }

    @AfterMethod
    public void releaseLogs() {
        julRoot.removeHandler(handler);
        julRoot.setLevel(previousLevel);
    }

    /**
     * The one that would matter at 3am: today an exhausted pool throws, and nothing anywhere says
     * why the application stopped doing work.
     */
    @Test
    public void poolExhaustionIsLoggedWithSomethingActionable() {
        LoggingTestProvider provider = new LoggingTestProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(provider, config(), 1,
                Duration.ofMinutes(5), Duration.ofMillis(150))) {

            SftpSession held = pool.borrow();
            assertThatThrownBy(pool::borrow).isInstanceOf(SftpException.class);
            pool.recycle(held);

            List<LogRecord> warnings = records.stream()
                    .filter(r -> r.getLevel().intValue() >= Level.WARNING.intValue())
                    .collect(Collectors.toList());

            assertThat(warnings)
                    .describedAs("an exhausted pool must say so at WARN, not just throw")
                    .isNotEmpty();
            assertThat(formatted(warnings))
                    .anySatisfy(message -> assertThat(message)
                            .contains("exhausted")
                            .contains("never returned it"));
        }
    }

    /** Opening a connection is the expensive, interesting event; it should be visible at DEBUG. */
    @Test
    public void openingASessionIsVisibleAtDebug() {
        LoggingTestProvider provider = new LoggingTestProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(provider, config(), 2,
                Duration.ofMinutes(5), Duration.ofSeconds(5))) {
            pool.recycle(pool.borrow());
        }
        assertThat(formatted(records))
                .anySatisfy(message -> assertThat(message).contains("Opening a new session"));
    }

    /**
     * The negative assertion, over a whole lifecycle: bootstrap, discovery, connect, operate, close.
     * A password or key passphrase must not appear in any record, at any level.
     */
    @Test
    public void noCredentialEverReachesTheLogs() {
        try (SftpClient client = SftpClients.blocking(LoggingTestProvider.NAME, config())) {
            client.list("/upload");
        }

        assertThat(records)
                .describedAs("expected the lifecycle to log something, or this proves nothing")
                .isNotEmpty();

        List<String> messages = formatted(records);
        assertThat(messages)
                .describedAs("a credential reached the logs")
                .noneMatch(message -> message.contains(PASSWORD))
                .noneMatch(message -> message.contains(PASSPHRASE));

        // The redaction that makes this safe is SftpConnectionConfig.toString(); confirm the config
        // really was logged, so this test fails if logging is removed rather than passing vacuously.
        assertThat(messages)
                .anySatisfy(message -> assertThat(message).contains("svc@sftp.example.com"));
    }

    private List<String> formatted(List<LogRecord> captured) {
        return captured.stream().map(record -> {
            String message = String.valueOf(record.getMessage());
            Object[] params = record.getParameters();
            if (params != null) {
                for (Object param : params) {
                    message = message + " | " + param;
                }
            }
            return message;
        }).collect(Collectors.toList());
    }

    private static SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
                .host("sftp.example.com")
                .username("svc")
                .password(PASSWORD.toCharArray())
                .privateKey(Path.of("/tmp/does-not-exist.pem"))
                .passphrase(PASSPHRASE.toCharArray())
                .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build();
    }
}
