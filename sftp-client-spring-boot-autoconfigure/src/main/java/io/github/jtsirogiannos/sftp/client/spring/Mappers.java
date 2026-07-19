package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;

/**
 * The one-way boundary between Spring's config model and the library's.
 *
 * <p>Its existence is the point: because this translation is a dozen lines in the starter, the core
 * never imports anything Spring-shaped, and a Quarkus user is not paying for Spring's binder. The
 * Quarkus runtime module has an equivalent of this file and nothing more.
 *
 * <p>Keep it dumb. If mapping an endpoint ever needs conditionals beyond "is a key configured", the
 * two config models have drifted and the fix belongs in {@link SftpClientProperties}, not here.
 */
final class Mappers {

    private Mappers() {
    }

    /**
     * Translates one bound endpoint into the library's config record.
     *
     * <p>Validation is deliberately delegated: {@link SftpConnectionConfig}'s compact constructor
     * already rejects a blank host, an out-of-range port, missing credentials, and a strategy that
     * needs a {@code known_hosts} file without one. Re-checking here would mean two sets of rules
     * drifting apart, and the record's messages are the ones worth surfacing.
     *
     * @throws IllegalArgumentException if the endpoint is incompletely configured — thrown at
     *                                  startup while the context is building, which is exactly when you want to hear it
     */
    static SftpConnectionConfig toConfig(SftpClientProperties.Endpoint endpoint) {
        SftpConnectionConfig.Builder builder = SftpConnectionConfig.builder()
            .host(endpoint.getHost())
            .port(endpoint.getPort())
            .username(endpoint.getUsername())
            .password(toCharArray(endpoint.getPassword()))
            .privateKey(endpoint.getPrivateKey())
            .passphrase(toCharArray(endpoint.getPassphrase()))
            .hostKeyStrategy(endpoint.getHostKeyStrategy())
            .connectTimeout(endpoint.getConnectTimeout())
            .requestTimeout(endpoint.getRequestTimeout());

        // Only override when configured. Passing null through would wipe the builder's
        // ~/.ssh/known_hosts default, and STRICT (the default strategy) rejects a null
        // known-hosts file - so every endpoint that didn't set one would fail to start.
        if (endpoint.getKnownHosts() != null) {
            builder.knownHosts(endpoint.getKnownHosts());
        }
        return builder.build();
    }

    /**
     * Null and blank both mean "not configured". Blank matters in practice: an unset
     * {@code ${SFTP_PASSWORD}} placeholder binds to an empty string, and passing that through would
     * defeat the record's "no credentials" check and fail later at connect time instead.
     */
    private static char[] toCharArray(String value) {
        return value == null || value.isEmpty() ? null : value.toCharArray();
    }
}
