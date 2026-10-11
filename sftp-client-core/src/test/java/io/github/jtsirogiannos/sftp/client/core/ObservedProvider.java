package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.provider.inmemory.InMemorySftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The in-memory provider, with every session it hands out visible to the test and breakable on
 * demand.
 *
 * <p>A real filesystem underneath, so a test observes what the core does with sessions rather than a
 * fake's idea of SFTP; a proxy on top, so it can also see which session was opened, whether it was
 * closed, and make one report itself broken the way a provider must after a desync.
 */
public final class ObservedProvider implements SftpProvider {

    private final InMemorySftpProvider real = new InMemorySftpProvider();
    public final List<Observed> sessions = new CopyOnWriteArrayList<>();

    public static SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
                .host("localhost")
                .username("test")
                .password("secret".toCharArray())
                .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build();
    }

    public BoundedSftpSessionPool<SftpSession> pool(int maxSize) {
        return BoundedSftpSessionPool.blocking(this, config(), maxSize, Duration.ofMinutes(5),
                Duration.ofMillis(300));
    }

    @Override
    public String name() {
        return "observed";
    }

    @Override
    public SftpCapabilities capabilities() {
        return real.capabilities();
    }

    @Override
    public SftpSession connect(SftpConnectionConfig config) {
        Observed observed = new Observed(real.connect(config), config.username());
        sessions.add(observed);
        return observed.proxy;
    }

    /** The most recently opened session. */
    public Observed last() {
        return sessions.get(sessions.size() - 1);
    }

    public static final class Observed {

        final SftpSession real;
        final SftpSession proxy;
        /** Which account the session was opened as — how a test tells tenants' connections apart. */
        public final String username;
        public volatile boolean broken;
        public volatile boolean closed;

        Observed(SftpSession real, String username) {
            this.real = real;
            this.username = username;
            this.proxy = (SftpSession) Proxy.newProxyInstance(SftpSession.class.getClassLoader(),
                    new Class<?>[] {SftpSession.class}, this::invoke);
        }

        private Object invoke(Object self, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "isValid":
                    return !broken && !closed && real.isValid();
                case "close":
                    closed = true;
                    break;
                case "equals":
                    return self == args[0];
                case "hashCode":
                    return System.identityHashCode(self);
                case "toString":
                    return "Observed[" + real + "]";
                default:
                    break;
            }
            try {
                return method.invoke(real, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }
}
