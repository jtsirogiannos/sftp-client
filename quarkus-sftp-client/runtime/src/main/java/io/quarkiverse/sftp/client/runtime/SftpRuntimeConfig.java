package io.quarkiverse.sftp.client.runtime;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Per-environment settings, read when the application starts.
 *
 * <p>One mapping describes the whole {@code sftp.endpoints.*} block. Splitting it into a build-time
 * half for endpoint names and a run-time half for credentials — the way Quarkus splits its own
 * datasource config — does not work when both claim the same prefix; see the note in
 * {@code SftpClientProcessor} for why. Keeping everything here is also what stops a host or a
 * credential being fixed into a native image.
 *
 * <p>This mirrors {@link io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig} field for field
 * and adds no concepts of its own. The library never learns what a Quarkus config property is.
 */
@ConfigMapping(prefix = "sftp")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface SftpRuntimeConfig {

    /**
     * Endpoints, keyed by the name used in {@code @NamedSftp}.
     */
    Map<String, EndpointRuntimeConfig> endpoints();

    interface EndpointRuntimeConfig {

        /**
         * Provider to resolve via ServiceLoader: `mina` or `jsch`. The matching
         * `quarkus-sftp-client-<name>` bundle must be a dependency.
         * <p>
         * Read during augmentation as well as at runtime, because it decides which driver has to be
         * on the classpath — so changing it means rebuilding, exactly like swapping a JDBC driver.
         */
        @WithDefault("mina")
        String provider();

        /**
         * Remote host name or address.
         */
        String host();

        /**
         * Remote port.
         */
        @WithDefault("22")
        int port();

        /**
         * User to authenticate as.
         */
        String username();

        /**
         * Password, or empty when authenticating by key.
         */
        Optional<String> password();

        /**
         * Private key file. Takes precedence over the password when both are set.
         */
        Optional<Path> privateKey();

        /**
         * Passphrase for the private key. Empty when the key is unencrypted.
         */
        Optional<String> passphrase();

        /**
         * How the server's host key is treated. Defaults to STRICT, matching the library: refuse an
         * unknown host rather than guess.
         */
        @WithDefault("STRICT")
        HostKeyStrategy hostKeyStrategy();

        /**
         * Known-hosts file to check the server key against. Defaults to {@code ~/.ssh/known_hosts}.
         */
        Optional<Path> knownHosts();

        /**
         * How long to wait for the connection and handshake to complete.
         */
        @WithDefault("PT10S")
        Duration connectTimeout();

        /**
         * How long to wait for a single request to complete once connected.
         */
        @WithDefault("PT30S")
        Duration requestTimeout();
    }
}
