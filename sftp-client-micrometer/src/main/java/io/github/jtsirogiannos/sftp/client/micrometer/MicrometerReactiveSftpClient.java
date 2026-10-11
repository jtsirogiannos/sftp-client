package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpOperations;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * The reactive counterpart to {@link MicrometerSftpClient}, reporting the same meters.
 *
 * <pre>{@code
 * ReactiveSftpClient client = MicrometerReactiveSftpClient.wrap(
 *         SftpClients.reactive("mina", config, worker), registry, config);
 * }</pre>
 *
 * <p><strong>Every measurement is taken on completion, not on return.</strong> A reactive operation
 * returns a {@link CompletionStage} immediately and finishes later, so stopping a timer where the
 * method returns would time the construction of a stage and report near-zero for every operation —
 * a metric that looks healthy precisely because it measures nothing. Timers stop in a
 * {@code whenComplete} callback, and the transfer counters come from the publisher's terminal
 * signal.
 *
 * <p><strong>A unit of work is measured too.</strong> {@code withSession} hands its work a session
 * whose operations would otherwise bypass this decorator, and {@code openSession} returns one; both
 * are wrapped with the same instrumentation.
 */
public final class MicrometerReactiveSftpClient extends ForwardingReactiveSftpOperations
        implements ReactiveSftpClient {

    private final ReactiveSftpClient client;
    private final MeterRegistry registry;
    private final Tags tags;
    private final ReactiveSftpOperations metered;

    private MicrometerReactiveSftpClient(ReactiveSftpClient client, MeterRegistry registry,
            Tags tags) {
        this.client = client;
        this.registry = registry;
        this.tags = tags;
        this.metered = new MeteredReactiveSftpOperations(client, registry, tags);
    }

    /** Wraps {@code client}, tagging meters with the endpoint. No credential is read. */
    public static ReactiveSftpClient wrap(ReactiveSftpClient client, MeterRegistry registry,
            SftpConnectionConfig config) {
        return wrap(client, registry, Tags.of(
            "host", config.host(),
            "port", Integer.toString(config.port()),
            "user", config.username()));
    }

    /** Wraps {@code client} with tags of your choosing. */
    public static ReactiveSftpClient wrap(ReactiveSftpClient client, MeterRegistry registry,
            Tags tags) {
        return new MicrometerReactiveSftpClient(client, registry, tags);
    }

    @Override
    protected ReactiveSftpOperations delegate() {
        return metered;
    }

    @Override
    public <R> CompletionStage<R> withSession(
            Function<ReactiveSftpOperations, ? extends CompletionStage<R>> work) {
        return client.withSession(operations ->
            work.apply(new MeteredReactiveSftpOperations(operations, registry, tags)));
    }

    @Override
    public CompletionStage<ReactiveSftpClientSession> openSession() {
        return client.openSession().thenApply(this::metered);
    }

    private ReactiveSftpClientSession metered(ReactiveSftpClientSession session) {
        ReactiveSftpOperations operations = new MeteredReactiveSftpOperations(session, registry, tags);
        return new ForwardingReactiveSftpClientSession() {
            @Override
            protected ReactiveSftpOperations delegate() {
                return operations;
            }

            @Override
            protected ReactiveSftpClientSession session() {
                return session;
            }
        };
    }

    /** The wrapped client's: a decorator observes sessions, it does not decide where they live. */
    @Override
    public SftpSessionContext sessionContext() {
        return client.sessionContext();
    }

    @Override
    public void close() {
        client.close();
    }
}
