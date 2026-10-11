package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.core.ForwardingSftpClientSession;
import io.github.jtsirogiannos.sftp.client.core.ForwardingSftpOperations;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link SftpClient} that reports what it does to a Micrometer registry.
 *
 * <pre>{@code
 * SftpClient client = MicrometerSftpClient.wrap(
 *         SftpClients.blocking("mina", config), registry, config);
 * }</pre>
 *
 * <p>A decorator rather than instrumentation inside the client, so the core never depends on
 * Micrometer and an application that does not want metrics pays nothing for them.
 *
 * <h2>What is measured</h2>
 *
 * <ul>
 *   <li>{@code sftp.client.operations} — a timer per operation, tagged {@code operation} and
 *       {@code outcome}. Convenience methods are not timed as units: {@code mkdirs} shows up as the
 *       {@code exists} and {@code mkdir} calls it actually made, because those are what crossed the
 *       wire.</li>
 *   <li>{@code sftp.client.transferred} — bytes, tagged {@code direction}. Counted from what moved,
 *       not from what was requested, so a failed transfer reports the part that landed.</li>
 * </ul>
 *
 * <p>Pool health is not here: it belongs to the pool rather than to a client, and is bound
 * separately with {@link SftpPoolMetrics}.
 *
 * <p><strong>A unit of work is measured too.</strong> {@code withSession} hands the block a
 * session-bound {@link SftpOperations} that would otherwise bypass this decorator entirely and go
 * unreported — the exact case where per-operation timing matters most, since that is where callers
 * put their multi-step work. The operations handed to the block are wrapped with the same
 * instrumentation, and so is a session from {@code openSession()}.
 */
public final class MicrometerSftpClient extends ForwardingSftpOperations implements SftpClient {

    private final SftpClient client;
    private final MeterRegistry registry;
    private final Tags tags;
    private final SftpOperations metered;

    private MicrometerSftpClient(SftpClient client, MeterRegistry registry, Tags tags) {
        this.client = client;
        this.registry = registry;
        this.tags = tags;
        this.metered = new MeteredSftpOperations(client, registry, tags);
    }

    /**
     * Wraps {@code client}, tagging every meter with the endpoint it belongs to.
     *
     * <p>The host, port and username come from the config because a process with several endpoints
     * produces meters that are otherwise indistinguishable. No credential is read.
     *
     * <p><strong>The {@code user} tag discloses service-account names to metrics readers</strong>,
     * which in many organisations is a wider audience than log access. That is usually fine — it is
     * an account name, not a secret — but a deployment with a stricter metrics policy should use
     * {@link #wrap(SftpClient, MeterRegistry, Tags)} and choose its own tags instead.
     */
    public static SftpClient wrap(SftpClient client, MeterRegistry registry,
            SftpConnectionConfig config) {
        return wrap(client, registry, Tags.of(
            "host", config.host(),
            "port", Integer.toString(config.port()),
            "user", config.username()));
    }

    /** Wraps {@code client} with tags of your choosing. */
    public static SftpClient wrap(SftpClient client, MeterRegistry registry, Tags tags) {
        return new MicrometerSftpClient(client, registry, tags);
    }

    @Override
    protected SftpOperations delegate() {
        return metered;
    }

    @Override
    public <R> R withSession(Function<SftpOperations, R> work) {
        return client.withSession(operations ->
            work.apply(new MeteredSftpOperations(operations, registry, tags)));
    }

    @Override
    public void withSessionWithoutResult(Consumer<SftpOperations> work) {
        client.withSessionWithoutResult(operations ->
            work.accept(new MeteredSftpOperations(operations, registry, tags)));
    }

    @Override
    public SftpClientSession openSession() {
        SftpClientSession session = client.openSession();
        SftpOperations metered = new MeteredSftpOperations(session, registry, tags);
        return new ForwardingSftpClientSession() {
            @Override
            protected SftpOperations delegate() {
                return metered;
            }

            @Override
            protected SftpClientSession session() {
                return session;
            }
        };
    }

    /** The wrapped client's: a decorator observes sessions, it does not decide where they live. */
    @Override
    public io.github.jtsirogiannos.sftp.client.context.SftpSessionContext sessionContext() {
        return client.sessionContext();
    }

    @Override
    public void close() {
        client.close();
    }
}
