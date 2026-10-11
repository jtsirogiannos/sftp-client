package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

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
         * `quarkus-sftp-client-&lt;name&gt;` bundle must be a dependency.
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

        /** Pool tuning for this endpoint, bound from {@code sftp.endpoints.<name>.pool.*}. */
        PoolConfig pool();

        /** Multi-tenancy for this endpoint, bound from {@code sftp.endpoints.<name>.tenancy.*}. */
        TenancyConfig tenancy();

        interface TenancyConfig {

            /**
             * Serve many tenants through this endpoint: its beans become a
             * {@code MultiTenantSftpClient} and a Mutiny client over a multi-tenant one.
             * <p>
             * Read during augmentation as well as at runtime, because it decides the bean types — so
             * changing it means rebuilding.
             */
            @WithDefault("false")
            boolean enabled();

            /**
             * Confine each tenant to a directory on this endpoint's connection: an absolute path
             * containing `{tenant}`, such as `/tenants/{tenant}`. Used when no `SftpTenantProvider`
             * bean decides instead.
             */
            Optional<String> rootDirectory();

            /** The pool each tenant with a connection of its own gets. */
            TenantPoolConfig pool();

            interface TenantPoolConfig {

                /** Most sessions a tenant's own pool holds; one pool per tenant, so kept small. */
                @WithDefault("2")
                int maxSize();

                /** How long a tenant's parked session may sit before being closed. */
                @WithDefault("PT5M")
                Duration maxIdle();

                /** How long a borrow from a tenant's pool waits at capacity before failing. */
                @WithDefault("PT30S")
                Duration acquireTimeout();
            }
        }

        interface PoolConfig {

            /**
             * Most sessions the pool holds — the ceiling on concurrent operations for this
             * endpoint. The ninth concurrent caller on the default of 8 waits for
             * {@code acquire-timeout} and then fails.
             */
            @WithDefault("8")
            int maxSize();

            /**
             * How long a parked session may sit before being closed instead of reused. Align with
             * the server's idle timeout if parked sessions keep dying.
             */
            @WithDefault("PT5M")
            Duration maxIdle();

            /** How long a borrow waits at capacity before failing loudly. */
            @WithDefault("PT30S")
            Duration acquireTimeout();
        }
    }
}
