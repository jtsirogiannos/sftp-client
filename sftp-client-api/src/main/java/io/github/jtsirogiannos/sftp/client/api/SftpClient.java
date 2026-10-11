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
     * Runs a block of work over a single connection.
     *
     * <p>Ordinarily each call borrows and returns a session independently, which is right for
     * one-shot operations and wasteful for a sequence of them: clearing a small tree costs fourteen
     * borrows, creating four nested directories costs ten. Inside this block they cost one, and
     * every operation is guaranteed to travel over the same connection.
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
     * Closes the pool and every idle session in it. Idempotent, and never throws — note the narrowed
     * signature, so {@code try}-with-resources needs no catch.
     */
    @Override
    void close();
}
