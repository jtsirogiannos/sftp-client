/**
 * The types callers use: client facades, the operation set, and the values they exchange.
 *
 * <p>Everything here is JDK-only. Adding this module to a classpath brings no SSH library, no
 * pooling, no reactive runtime and no framework — a backend is chosen separately, the way a JDBC
 * driver is.
 *
 * <h2>Getting a client</h2>
 *
 * <p>The bootstrap lives in {@code sftp-client-core}, because discovery and pooling are not part of
 * the contract a provider implements:
 *
 * <pre>{@code
 * try (SftpClient client = SftpClients.blocking("mina", config)) {
 *     client.upload("/upload/orders.csv", data);
 * }
 * }</pre>
 *
 * <p>{@link io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig} carries the target and
 * credentials, and its {@code toString()} is redacted so a logged config cannot leak a password or a
 * passphrase. {@link io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy} decides how the host
 * key is trusted; the permissive value is named {@code INSECURE_ACCEPT_ANY} on purpose.
 *
 * <h2>The two facades</h2>
 *
 * <p>{@link io.github.jtsirogiannos.sftp.client.api.SftpOperations} is the operation set both
 * facades share — metadata, transfers, links, attributes — plus convenience built entirely from
 * those primitives ({@code mkdirs}, {@code deleteRecursively}, {@code visit}, {@code findFirst},
 * {@code uploadAtomically}, {@code java.nio.file.Path} transfers).
 *
 * <ul>
 *   <li>{@link io.github.jtsirogiannos.sftp.client.api.SftpClient} — blocking, and adds
 *       {@code withSession}, which pins one connection for a block of work so a sequence of calls
 *       costs one borrow instead of one per operation.</li>
 *   <li>{@link io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient} — the same operations in
 *       {@link java.util.concurrent.CompletionStage} and {@link java.util.concurrent.Flow}, so
 *       Reactor, Mutiny and coroutines each adapt with a one-line shim rather than a second
 *       implementation.</li>
 * </ul>
 *
 * <h2>Reading the answers</h2>
 *
 * <p>{@link io.github.jtsirogiannos.sftp.client.api.SftpException} is the only failure type callers
 * catch, whichever backend is underneath. It carries the server's {@code SSH_FX_*} status where
 * there was one, so {@code isNoSuchFile()} beats matching on a message.
 *
 * <p>{@link io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities} is worth consulting before
 * assuming an operation exists. Backends genuinely differ — SFTP v3 has no rename that replaces an
 * existing file, and not every client library exposes an exclusive create — and the flags say what
 * is actually available rather than what would be convenient. Anything a session cannot do throws
 * {@link java.lang.UnsupportedOperationException} rather than silently doing something weaker.
 */
package io.github.jtsirogiannos.sftp.client.api;
