package io.github.jtsirogiannos.sftp.client.api;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The blocking facade — the one most users want. Sessions are borrowed from a pool per call and
 * returned automatically, so this is safe to hold as a long-lived singleton and to share across
 * threads (unlike {@link io.github.jtsirogiannos.sftp.client.spi.SftpSession}, which is single-owner).
 *
 * <pre>{@code
 * try (var client = SftpClients.blocking("mina", config)) {
 *     client.list("/inbound").forEach(System.out::println);
 * }
 * }</pre>
 *
 * <p>Every operation lives on {@link SftpOperations}; this interface adds only the two things that
 * belong to owning a pool — closing it, and lending a single session to a block of work.
 *
 * <p>Every method here has a {@link ReactiveSftpClient} twin. The two facades are deliberately
 * separate types rather than one interface with two return shapes: a Spring MVC user should never
 * have to look at a {@code CompletionStage}, and an event-loop user should never be able to call a
 * blocking method by accident.
 */
public interface SftpClient extends SftpOperations, AutoCloseable {

    /**
     * Runs a block of work over a single connection, and makes that connection the current session
     * for this client while the block runs.
     *
     * <p>Ordinarily each call borrows and returns a session independently, which is right for
     * one-shot operations and wasteful for a sequence of them: clearing a small tree costs fourteen
     * borrows, creating four nested directories costs ten. Inside this block they cost one, and
     * every operation is guaranteed to travel over the same connection.
     *
     * <h4>The current session</h4>
     *
     * <p>Hibernate's {@code getCurrentSession()}, for SFTP. While the block runs, calls on
     * <em>this client</em> — not only on the operations handed to the block — join its session, so a
     * service deep in the call stack that was handed the injected client takes part without being
     * passed anything. A nested {@code withSession} joins too, rather than borrowing a second
     * connection and, on a small pool, waiting for itself. Where "current" is remembered is the
     * {@link #sessionContext()}'s business: by default the calling thread, so a call from another
     * thread borrows as usual.
     *
     * <p>Two kinds of call never join. {@link #download(String)} borrows its own connection, because
     * the stream it returns outlives the call and must not outlive the block's session with it. And
     * while a stream opened through the block's session is still being read, calls borrow
     * separately rather than interleave with the read. Each such borrow can wait for a pool slot like
     * any other.
     *
     * <pre>{@code
     * client.withSessionWithoutResult(ops -> {
     *     ops.mkdirs("/outbound/2026/07");
     *     ops.upload("/outbound/2026/07/report.csv", data);
     *     ops.chmod("/outbound/2026/07/report.csv", 0600);
     * });
     * }</pre>
     *
     * <p><strong>This is not a transaction.</strong> The name "session" is borrowed from Hibernate
     * and the resemblance stops at connection affinity: SFTP has no notion of rollback, so if the
     * third operation fails the first two have already happened and nothing undoes them. What you
     * get is one connection, not atomicity.
     *
     * <p>Anything the block throws reaches the caller unchanged; it is not wrapped in
     * {@link SftpException}. When the block ends, normally or not, the connection goes back to the
     * pool if the provider still vouches for it and no stream opened inside is left open, and is
     * discarded otherwise — so a business exception thrown mid-block costs nothing, and a broken
     * connection is never handed to the next caller. Anything stream-shaped opened inside, such as
     * {@link SftpOperations#download(String)}, is only valid until the block returns.
     */
    <R> R withSession(Function<SftpOperations, R> work);

    /**
     * {@link #withSession(Function)} for work that produces no value.
     *
     * <p>Separately named rather than overloaded, because Java cannot resolve a lambda between
     * {@code Consumer} and {@code Function} overloads when its body is a value-returning call:
     * {@code client.withSession(ops -> ops.list("/inbound"))} would simply fail to compile. Spring's
     * {@code TransactionTemplate} carries {@code executeWithoutResult} for the same reason.
     */
    void withSessionWithoutResult(Consumer<SftpOperations> work);

    /**
     * Opens a session the caller manages: one connection, held until the returned session is closed.
     *
     * <p>The explicit form of {@link #withSession}, for when a lambda is the wrong shape — the work
     * throws checked exceptions, or spans several methods, or the session is handed to something
     * else. Use it with {@code try}-with-resources; an unclosed session holds a pooled connection
     * until the process ends.
     *
     * <p>Every call opens a <em>new</em> session on its own connection, so this is also how to get a
     * second connection on purpose — two transfers that must overlap, say. Unlike
     * {@link #withSession}, the session is never made current: calls on the client itself do not
     * join it. That is Hibernate's {@code openSession()} as against {@code getCurrentSession()}, and
     * it is what makes a session safe to hand to another thread.
     *
     * @throws SftpException if no connection can be had — the pool is exhausted past its acquire
     *                       timeout, or a new one cannot be opened
     */
    SftpClientSession openSession();

    /**
     * Where this client looks for, and {@link #withSession} records, the current session.
     *
     * <p>Exposed so that adapters and interceptors bind through the same context the client reads —
     * a Reactor or coroutine adapter carrying bindings in its own context replays them here.
     */
    io.github.jtsirogiannos.sftp.client.context.SftpSessionContext sessionContext();

    /**
     * Closes the pool and every idle session in it. Idempotent, and never throws — note the narrowed
     * signature, so {@code try}-with-resources needs no catch.
     */
    @Override
    void close();
}
