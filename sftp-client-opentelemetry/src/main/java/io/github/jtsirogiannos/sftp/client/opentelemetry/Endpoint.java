package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * The connection facts every span carries, and nothing else.
 *
 * <p>Built from {@link SftpConnectionConfig}, deliberately reading only host and port. A span
 * attribute is exported to a collector and shown in a UI, so a credential reaching one is a
 * credential leaked — this type exists partly so that reading anything else would be a visible
 * change rather than an easy mistake.
 */
final class Endpoint {

    private final String host;
    private final int port;
    private final Supplier<Optional<String>> protocolVersion;

    Endpoint(String host, int port, Supplier<Optional<String>> protocolVersion) {
        this.host = host;
        this.port = port;
        this.protocolVersion = protocolVersion;
    }

    static Endpoint of(SftpConnectionConfig config, Supplier<Optional<String>> protocolVersion) {
        return new Endpoint(config.host(), config.port(), protocolVersion);
    }

    String host() {
        return host;
    }

    int port() {
        return port;
    }

    /**
     * The negotiated SFTP version, when one is known.
     *
     * <p>Resolved lazily and per span rather than captured up front: the provider's advertised
     * ceiling is not what a given server agreed to, and asking before any session exists would
     * report the wrong number confidently.
     */
    Optional<String> protocolVersion() {
        return protocolVersion.get();
    }
}
