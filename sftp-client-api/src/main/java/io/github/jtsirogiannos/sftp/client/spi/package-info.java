/**
 * The service provider interface: everything a backend author implements, and nothing else.
 *
 * <p>This package is the whole contract between the library and an SFTP backend, in the same way
 * {@code java.sql.Driver} is the contract between an application and a database. It has no
 * dependencies beyond the JDK, so a provider never inherits a framework, a pooling library, or a
 * reactive runtime by depending on it.
 *
 * <h2>What to implement</h2>
 *
 * <p>Two types, in this order:
 *
 * <ol>
 *   <li>{@link io.github.jtsirogiannos.sftp.client.spi.SftpProvider} — names the backend, declares
 *       what it can do, and opens sessions.</li>
 *   <li>{@link io.github.jtsirogiannos.sftp.client.spi.SftpSession} — one live channel, eighteen
 *       blocking operations.</li>
 * </ol>
 *
 * <p>A backend that can complete operations without parking a thread may additionally implement
 * {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider} and
 * {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession}. That is optional, independent of
 * the above, and today no third-party SSH library offers a client API that makes it possible — see
 * {@code docs/adr/0003-async-spi-and-the-mina-seam.md}.
 *
 * <h2>What not to implement</h2>
 *
 * <p>Connection pooling, retry, backpressure, publishers, executors, and the convenience operations
 * ({@code mkdirs}, {@code deleteRecursively}, {@code findFirst}, path transfers) all live in the
 * core and are built on the primitives here. A provider that finds itself writing any of them has
 * taken on work the library already does.
 *
 * <h2>Registration</h2>
 *
 * <p>Discovery is {@link java.util.ServiceLoader}. Ship a descriptor at
 * {@code META-INF/services/io.github.jtsirogiannos.sftp.client.spi.SftpProvider} naming the
 * implementation class, which must have a public no-arg constructor and be cheap to construct — it
 * is instantiated to answer {@code name()} before anyone decides to use it.
 *
 * <p>{@link io.github.jtsirogiannos.sftp.client.spi.SftpProvider#name()} is the key users type into
 * configuration, so it is public API: changing it breaks consumers. By convention it matches the
 * artifact suffix, which is what lets a "provider not found" error suggest
 * {@code sftp-provider-<name>}.
 *
 * <h2>Rules that cut across every operation</h2>
 *
 * <dl>
 *   <dt><strong>Failures surface as {@link io.github.jtsirogiannos.sftp.client.api.SftpException}</strong></dt>
 *   <dd>A backend exception reaching a caller makes the choice of provider visible in user code,
 *       which is the one thing this design exists to prevent. Where the server supplied a status,
 *       pass the {@code SSH_FX_*} code through, so callers can tell "no such file" from "permission
 *       denied" without parsing English.</dd>
 *
 *   <dt><strong>A session is single-owner and need not be thread-safe</strong></dt>
 *   <dd>Exactly one caller holds a session between borrow and return, so a single-threaded backend
 *       channel can be wrapped without adding locks.</dd>
 *
 *   <dt><strong>A recoverable failure must leave the session usable</strong></dt>
 *   <dd>A {@code stat} on a missing path is an ordinary answer, not a broken channel. Only a
 *       transport-level failure may make {@link
 *       io.github.jtsirogiannos.sftp.client.spi.SftpSessionLifecycle#isValid()} false.</dd>
 *
 *   <dt><strong>{@code isValid()} must not make a round trip</strong></dt>
 *   <dd>The pool calls it inline, on whatever thread asked for a session, including an event loop.
 *       Report whether the channel is open, not whether the server is reachable.</dd>
 *
 *   <dt><strong>{@code close()} is idempotent and never throws</strong></dt>
 *   <dd>It is called on cleanup paths that are already handling a failure.</dd>
 *
 *   <dt><strong>Paths are absolute</strong></dt>
 *   <dd>Operations take absolute remote paths, and every {@link
 *       io.github.jtsirogiannos.sftp.client.api.RemoteFile} a listing produces carries one that can
 *       be passed straight back in. {@code realpath} is how a caller discovers the login directory.
 *       Listings must not include {@code "."} or {@code ".."}.</dd>
 *
 *   <dt><strong>Capabilities describe observable behaviour, never aspiration</strong></dt>
 *   <dd>See {@link io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities}. A session must never
 *       claim more than its provider. Where a backend's public API cannot honour something, report
 *       the flag false and throw {@link java.lang.UnsupportedOperationException} — do not emulate it
 *       with a racy sequence of other calls, because a caller who checked the flag has already
 *       decided what to do.</dd>
 * </dl>
 *
 * <h2>Knowing when it works</h2>
 *
 * <p>The contract is executable. Add {@code sftp-client-tck} at test scope and extend
 * {@code SftpProviderConformanceTest}, supplying a provider and a session; that is the whole
 * integration. Its roughly thirty tests are the specification these paragraphs summarise — dot
 * entries, EOF, offsets, symlink versus target, attribute updates that leave unset fields alone,
 * capability consistency, and cleanliness after an abandoned transfer. Running it against a second
 * server implementation is worth the trouble: several bugs in the shipped providers passed against
 * one server and failed against another.
 *
 * <h2>A note on the protocol</h2>
 *
 * <p>SFTP is not an RFC. It is a series of expired Internet-Drafts,
 * {@code draft-ietf-secsh-filexfer-00} through {@code -13}; version 3 (draft-02) is what OpenSSH
 * speaks and version 6 (draft-13) is the ceiling here. Servers differ in ways the drafts leave open,
 * which is why {@link io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities} is negotiated per
 * session rather than declared once per backend.
 */
package io.github.jtsirogiannos.sftp.client.spi;
