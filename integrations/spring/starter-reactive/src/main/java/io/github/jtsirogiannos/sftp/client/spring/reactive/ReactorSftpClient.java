package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.context.SessionBindings;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Reactor view of the neutral {@link ReactiveSftpClient}, for WebFlux applications.
 *
 * <p>Its thinness is the point, and the second proof that the core is genuinely framework-neutral —
 * the Quarkus module makes the same claim with Mutiny. {@code Mono.fromCompletionStage} takes the
 * facade's {@code CompletionStage} directly; the streaming side needs one adapter call, because
 * Reactor consumes {@code org.reactivestreams.Publisher} while the facade speaks
 * {@code java.util.concurrent.Flow.Publisher}. That bridge is {@link FlowAdapters}, and it is the
 * whole of the impedance mismatch.
 *
 * <h2>Units of work</h2>
 *
 * <p>{@link #withSession} runs a unit of work over one connection, and its session is current for
 * the work's whole pipeline — carried in the Reactor {@code Context}, which the library cannot read
 * by itself, and replayed onto the library's session context around every call. So a call made on
 * this client anywhere inside the work joins it, whichever thread a step runs on.
 *
 * <h2>Tenants</h2>
 *
 * <p>Over a {@link MultiTenantReactiveSftpClient}, a pipeline names its tenant with
 * {@code .contextWrite(SftpReactorContext.tenant("acme"))}; without one, the client's own resolver
 * decides.
 *
 * <p><strong>These transfers are not non-blocking I/O, and a WebFlux caller should know it.</strong>
 * A {@code Mono} or {@code Flux} usually promises that no thread is parked waiting on the network.
 * Here one is: every transfer occupies a thread from the client's worker executor for its duration,
 * because neither backend exposes a zero-thread SFTP path — the same reason
 * {@link SftpCapabilities#pipelinedStreaming()} is not called "async".
 *
 * <p>What that means in practice:
 * <ul>
 *   <li>Concurrent transfers are capped by the worker pool (eight threads by default), not by
 *       demand. The ninth concurrent download waits for a thread, and its {@code Mono} simply takes
 *       longer to emit.</li>
 *   <li>Nothing blocks the event loop: the work is already dispatched to that pool, and signals
 *       arrive on it. No {@code publishOn} is applied here — adding one would buy a thread hop and
 *       no safety.</li>
 *   <li>Size the executor for the transfers you expect to overlap, not for request throughput.</li>
 * </ul>
 */
public class ReactorSftpClient extends ReactorSftpOperations implements AutoCloseable {

    private final ReactiveSftpClient delegate;

    public ReactorSftpClient(ReactiveSftpClient delegate) {
        super(delegate, delegate.sessionContext());
        this.delegate = delegate;
    }

    /**
     * The neutral client underneath, exposed because this wrapper is maintained by hand and lags
     * behind whenever the neutral client grows — it once lagged by thirteen operations. The escape
     * hatch keeps that a nuisance rather than a wall.
     */
    public ReactiveSftpClient unwrap() {
        return delegate;
    }

    @Override
    ReactiveSftpOperations target(ContextView subscriber) {
        return client(subscriber);
    }

    /** The subscriber's tenant's client, when it names one and there are tenants to name. */
    private ReactiveSftpClient client(ContextView subscriber) {
        Object tenant = subscriber.getOrDefault(SftpReactorContext.TENANT, null);
        return tenant != null && delegate instanceof MultiTenantReactiveSftpClient multiTenant
            ? multiTenant.forTenant((String) tenant)
            : delegate;
    }

    /**
     * Runs a unit of work over one connection, with that session current for the work's whole
     * pipeline.
     *
     * <pre>{@code
     * client.withSession(ops -> ops.mkdirs("/outbound/2026/10")
     *         .then(ops.upload("/outbound/2026/10/report.csv", data))
     *         .then(archiver.archive()));   // calls on the client inside join this session
     * }</pre>
     *
     * <p>A nested {@code withSession} joins. The work's own error propagates unchanged; the
     * connection goes back to the pool if it is still sound. Cancelling the returned {@code Mono}
     * cancels the work and then hands the connection back.
     */
    public <R> Mono<R> withSession(Function<ReactorSftpOperations, ? extends Mono<R>> work) {
        return Mono.deferContextual(outer -> {
            ReactiveSftpClient client = client(outer);
            AtomicReference<CompletableFuture<R>> running = new AtomicReference<>();
            CompletableFuture<R> outcome = context.callWith(SftpReactorContext.bindings(outer), () ->
                client.<R>withSession(operations -> {
                    CompletableFuture<R> future = within(work.apply(
                        new ReactorSftpOperations(operations, context)), outer).toFuture();
                    running.set(future);
                    return future;
                }).toCompletableFuture());
            return Mono.fromFuture(outcome, true)
                .onErrorMap(CompletionException.class, e -> e.getCause() != null ? e.getCause() : e)
                .doOnCancel(() -> {
                    CompletableFuture<R> future = running.get();
                    if (future != null) {
                        future.cancel(true); // disposes the work's subscription
                    }
                });
        });
    }

    /**
     * {@link #withSession} for work that emits many items.
     *
     * <p>Items are relayed as the work emits them, but demand is not: the work runs to completion
     * over its session whatever the downstream requests, and what it emits faster than that is
     * buffered. A unit of work is short; for a stream worth backpressure, transfer outside one.
     */
    public <R> Flux<R> withSessionMany(Function<ReactorSftpOperations, ? extends Publisher<R>> work) {
        return Flux.deferContextual(outer -> Flux.<R>create(sink -> {
            ReactiveSftpClient client = client(outer);
            AtomicReference<CompletableFuture<Void>> running = new AtomicReference<>();
            CompletableFuture<Void> outcome = context.callWith(SftpReactorContext.bindings(outer), () ->
                client.<Void>withSession(operations -> {
                    CompletableFuture<Void> future = within(
                        Flux.from(work.apply(new ReactorSftpOperations(operations, context)))
                            .doOnNext(sink::next)
                            .then(), outer).toFuture();
                    running.set(future);
                    return future;
                }).toCompletableFuture());
            sink.onCancel(() -> {
                CompletableFuture<Void> future = running.get();
                if (future != null) {
                    future.cancel(true);
                }
            });
            outcome.whenComplete((done, failure) -> {
                if (failure == null) {
                    sink.complete();
                } else {
                    sink.error(failure instanceof CompletionException && failure.getCause() != null
                        ? failure.getCause() : failure);
                }
            });
        }));
    }

    /**
     * The work's pipeline, run with the outer subscriber's context plus the new session — captured
     * from the library's context at the one moment it is current there: inside the neutral
     * {@code withSession}, on this thread.
     */
    private <T> Mono<T> within(Mono<T> pipeline, ContextView outer) {
        SessionBindings inner = context.capture();
        return pipeline.contextWrite(c -> c.putAll(outer).put(SftpReactorContext.BINDINGS, inner));
    }

    /** Opens a session the caller manages; never current, never joined. */
    public Mono<ReactorSftpSession> openSession() {
        return Mono.deferContextual(subscriber -> Mono.fromCompletionStage(client(subscriber).openSession()))
            .map(session -> new ReactorSftpSession(session, context));
    }

    /** Where the underlying client looks for its current session. */
    public SftpSessionContext sessionContext() {
        return context;
    }

    @Override
    public SftpCapabilities capabilities() {
        return delegate.capabilities();
    }

    /**
     * Closes the underlying client and its connection pool. This wrapper owns the delegate it was
     * given, so disposing the bean releases the endpoint's connections.
     */
    @Override
    public void close() {
        delegate.close();
    }
}
