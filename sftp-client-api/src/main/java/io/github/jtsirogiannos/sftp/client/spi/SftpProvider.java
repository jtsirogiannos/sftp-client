package io.github.jtsirogiannos.sftp.client.spi;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;

/**
 * A backend implementation — the JDBC {@code Driver} of this library. Two methods of metadata and
 * one factory method; everything else a caller needs is on {@link SftpSession}.
 *
 * <p>Providers are discovered with {@link java.util.ServiceLoader}, so an implementation ships a
 * {@code META-INF/services/io.github.jtsirogiannos.sftp.client.spi.SftpProvider} file naming the class,
 * and putting the jar on the classpath is all it takes to make {@link #name()} resolvable. That file
 * is why {@code sftp-provider-mina} needs no wiring code and the Quarkus/Spring "bundle" artifacts
 * can be code-free.
 *
 * <p>Implementations must have a public no-arg constructor and must be cheap to construct —
 * {@code ServiceLoader} instantiates every provider on the classpath just to read its name, so
 * connecting or allocating threads in a constructor would make provider lookup expensive and
 * failure-prone.
 */
public interface SftpProvider {

    /**
     * The short, stable lookup key: {@code "jsch"}, {@code "mina"}. Users type this into config, so
     * it is public API — changing it is a breaking change. By convention it matches the artifact
     * suffix, which is what lets the "provider not found" error suggest
     * {@code sftp-provider-<name>}.
     */
    String name();

    /**
     * What this backend can do. Must be constant for the life of the provider and must not require a
     * connection to answer — the core calls this to size read buffers before any session exists.
     */
    SftpCapabilities capabilities();

    /**
     * Opens a new authenticated session, blocking until the handshake completes or
     * {@link SftpConnectionConfig#connectTimeout()} elapses.
     *
     * <p>Each call returns an independent session; pooling is the core's job, not the provider's.
     *
     * @throws SftpException if the host is unreachable, authentication fails, or the host key is
     *                       rejected under the configured {@link io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy}
     */
    SftpSession connect(SftpConnectionConfig config);
}
