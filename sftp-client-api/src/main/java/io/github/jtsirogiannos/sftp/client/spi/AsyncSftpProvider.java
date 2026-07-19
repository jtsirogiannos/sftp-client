package io.github.jtsirogiannos.sftp.client.spi;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;

/**
 * A backend that can additionally open {@linkplain AsyncSftpSession thread-free sessions}.
 *
 * <p>This one <em>does</em> extend {@link SftpProvider}, unlike {@link AsyncSftpSession}, and the
 * asymmetry is deliberate rather than an oversight. The tax that made inheritance wrong at the
 * session level was its size: nineteen blocking methods, two of them stream-shaped and genuinely hard
 * to satisfy. A provider owes exactly one extra method here — {@link #connect} — and in exchange the
 * whole discovery path stays untouched: one {@code ServiceLoader} descriptor, one registry, one
 * {@code name()} lookup, and the existing Spring and Quarkus wiring keep working with no knowledge of
 * any of this.
 *
 * <p>So a provider that is genuinely both — the in-memory one, where "without blocking" is simply
 * true — implements both {@code connect} and {@link #connectAsync} directly. A provider that is
 * async-only supplies a blocking view from its async session for {@code connect}, which is what keeps
 * it usable from {@code SftpClient}, {@code withSession} and the conformance suite.
 */
public interface AsyncSftpProvider extends SftpProvider {

    /**
     * Opens a session that completes operations without parking a thread.
     *
     * <p>Opening itself may still block on the SSH handshake — that is a one-off cost the pool pays
     * off the caller's thread. It is the operations afterwards that must not park anything.
     *
     * @throws io.github.jtsirogiannos.sftp.client.api.SftpException on the same terms as {@link #connect}
     */
    AsyncSftpSession connectAsync(SftpConnectionConfig config);
}
