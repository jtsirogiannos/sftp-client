package io.github.jtsirogiannos.sftp.client.api;

/**
 * An open session: every operation, pinned to one connection until {@link #close()}.
 *
 * <p>Obtained from {@link SftpClient#openSession()}, and the counterpart of Hibernate's
 * {@code openSession()}: the caller opens it, the caller closes it, and nothing else knows it exists.
 * {@code try}-with-resources is the intended shape, and unlike a lambda it lets checked exceptions
 * through without wrapping:
 *
 * <pre>{@code
 * try (SftpClientSession session = client.openSession()) {
 *     session.mkdirs("/outbound/2026/10");
 *     session.upload("/outbound/2026/10/report.csv", Files.newInputStream(report));
 * }
 * }</pre>
 *
 * <p><strong>This is not a transaction.</strong> SFTP has no rollback; work done before a failure
 * stays done. What a session buys is one connection — one handshake, one authentication — for a
 * sequence of operations, and the guarantee that they all travel over it.
 *
 * <h2>Rules</h2>
 *
 * <ul>
 *   <li><strong>One reader at a time.</strong> While a stream from {@link #download(String)} is
 *       open, every other call on this session throws {@link IllegalStateException}. Several SFTP
 *       clients — JSch among them — share one request queue per channel, so interleaving a read
 *       with anything else hands each side the other's replies. Close the stream first, or open a
 *       second session when two transfers genuinely need to overlap.</li>
 *   <li><strong>Closing decides the connection's fate.</strong> It goes back to the pool if the
 *       provider still vouches for it and no stream opened here is left open, and is discarded
 *       otherwise. A failed operation the server answered — a missing file, a refused permission —
 *       leaves the connection reusable.</li>
 *   <li><strong>Single-owner.</strong> Not thread-safe, like the connection behind it.</li>
 * </ul>
 */
public interface SftpClientSession extends SftpOperations, AutoCloseable {

    /** True until {@link #close()} has been called. */
    boolean isOpen();

    /**
     * Ends the session and hands its connection back to the pool, or discards it — see the rules
     * above. Idempotent, and never throws; note the narrowed signature, so {@code try}-with-resources
     * needs no catch. Any operation afterwards throws {@link IllegalStateException}.
     */
    @Override
    void close();
}
