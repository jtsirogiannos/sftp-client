package io.github.jtsirogiannos.sftp.client.spi;

/**
 * What every session has regardless of how it is driven: capabilities, a liveness check, and a close.
 *
 * <p>This exists because it is <em>exactly</em> what the pool needs. {@code BoundedSftpSessionPool}
 * calls {@link #isValid()} and {@link #close()} and nothing else — it never invokes a single SFTP
 * operation, because pooling is about the lifetime of a connection, not about what travels over it.
 * Naming that sliver is what lets one pool implementation serve both {@link SftpSession} and
 * {@link AsyncSftpSession} without either knowing the other exists.
 *
 * <p>The two session contracts are therefore siblings rather than one extending the other. Making
 * {@code AsyncSftpSession} a subtype of {@link SftpSession} would force an async provider to
 * implement nineteen blocking methods it has no use for, two of them stream-shaped and unsatisfiable
 * without a bounded {@code Flow.Publisher}-to-{@code InputStream} bridge. Providers implement the one
 * they actually are.
 */
public interface SftpSessionLifecycle extends AutoCloseable {

    /**
     * What this session can actually do, after negotiation with <em>this</em> server.
     *
     * <p>Distinct from {@link SftpProvider#capabilities()}, which describes the backend's best case
     * and cannot know what it is talking to. The difference is not academic: the MINA provider
     * negotiates SFTP v6 against MINA's own server and v3 against OpenSSH, and v3 has no atomic
     * replacing rename. A caller that trusted the provider-level answer would issue an operation the
     * server cannot perform — which is precisely what running the conformance suite against a second
     * server implementation exposed.
     *
     * <p>Providers whose behaviour does not vary by server may simply return the provider value.
     */
    SftpCapabilities capabilities();

    /**
     * Cheap liveness check used by the pool before handing a session out. Must not perform a network
     * round-trip; it reports whether the channel is still open, not whether the server is reachable.
     *
     * <p>A session must stay valid after a *recoverable* failure — a stat on a missing path leaves
     * the channel perfectly usable, and the TCK checks exactly that. Only transport-level failures
     * invalidate a session.
     *
     * <p>The no-round-trip rule is load-bearing beyond politeness: the pool's non-blocking
     * {@code borrowAsync} fast path calls this inline, on whatever thread asked for a session. A
     * provider that phoned the server here would block an event loop.
     */
    boolean isValid();

    /**
     * Closes the channel. Idempotent, and never throws — closing is not a failure mode worth propagating.
     */
    @Override
    void close();
}
