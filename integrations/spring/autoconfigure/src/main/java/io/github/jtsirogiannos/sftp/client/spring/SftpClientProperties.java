package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Binds {@code sftp.endpoints.<name>.*} from the environment.
 *
 * <pre>{@code
 * sftp:
 *   endpoints:
 *     bank-a:
 *       provider: mina
 *       host: sftp.bank-a.example.com
 *       username: svc
 *       password: ${BANK_A_PASSWORD}
 *     bank-b:
 *       provider: jsch
 *       host: sftp.bank-b.example.com
 *       username: svc
 *       private-key: /run/secrets/bank-b.pem
 *       host-key-strategy: accept_new
 * }</pre>
 *
 * <p>This class is the entire Spring-specific config surface, and it deliberately mirrors
 * {@link io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig} field for field rather than
 * adding concepts of its own. The library never learns what a Spring property is — {@link Mappers}
 * does the one-way translation at startup. The Quarkus {@code @ConfigMapping} interface is the same
 * shape, which is what makes the "port the sample app between frameworks and only the config
 * differs" exit criterion in ROADMAP Phase 4 achievable.
 *
 * <p>JavaBean accessors rather than constructor binding, because the auto-configuration reads
 * {@code getEndpoints()} and Spring must be able to populate the map incrementally.
 */
@ConfigurationProperties(prefix = SftpClientProperties.PREFIX)
public class SftpClientProperties {

    public static final String PREFIX = "sftp";

    /**
     * Binds straight off the environment, for callers that run before {@code @ConfigurationProperties}
     * does — bean-definition post-processors, which is how the clients get registered.
     *
     * <p>Shared so the servlet and reactive registrars cannot drift in how they discover endpoints.
     */
    public static SftpClientProperties bind(Environment environment) {
        return Binder.get(environment)
                .bind(PREFIX, SftpClientProperties.class)
                .orElseGet(SftpClientProperties::new);
    }

    /**
     * One entry per named endpoint. The map key is the endpoint name used for the bean name
     * ({@code sftpClient-bank-a}) and is the Spring equivalent of {@code @NamedSftp("bank-a")}.
     */
    private Map<String, Endpoint> endpoints = new LinkedHashMap<>();

    public Map<String, Endpoint> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(Map<String, Endpoint> endpoints) {
        this.endpoints = endpoints;
    }

    /**
     * Settings for a single remote server.
     */
    public static class Endpoint {

        /**
         * Provider name to resolve via {@code ServiceLoader}: {@code mina} or {@code jsch}. The
         * matching {@code sftp-provider-*} artifact must be on the classpath — {@code SftpClients}
         * throws with the list of what it did find if it is not.
         */
        private String provider = "mina";

        private String host;

        private int port = 22;

        private String username;

        /**
         * Password, or null when authenticating by key.
         *
         * <p>A {@code String} because that is what the environment hands us — Spring has already
         * materialised the value as one before this setter runs, so a {@code char[]} field here
         * would be security theatre. {@link Mappers} converts to {@code char[]} at the boundary,
         * which is where the library's own copy-and-wipe discipline takes over.
         */
        private String password;

        /**
         * Path to a private key file. Takes precedence over {@link #password} when both are set.
         */
        private Path privateKey;

        /**
         * Passphrase for {@link #privateKey}; null if the key is unencrypted.
         */
        private String passphrase;

        /**
         * Defaults to STRICT, matching the library default: refuse rather than guess.
         */
        private HostKeyStrategy hostKeyStrategy = HostKeyStrategy.STRICT;

        /**
         * Defaults to {@code ~/.ssh/known_hosts} when left unset.
         */
        private Path knownHosts;

        private Duration connectTimeout = Duration.ofSeconds(10);

        private Duration requestTimeout = Duration.ofSeconds(30);

        /**
         * Pool tuning, bound from {@code sftp.endpoints.<name>.pool.*}. Defaults match the core:
         * 8 sessions, 5 minutes idle, 30 seconds to acquire. The ninth concurrent operation on a
         * default-sized pool waits and then fails — size {@code max-size} for the operations you
         * expect to overlap.
         */
        private final Pool pool = new Pool();

        public Pool getPool() {
            return pool;
        }

        /**
         * Multi-tenancy for this endpoint, bound from {@code sftp.endpoints.<name>.tenancy.*}. When
         * enabled, the endpoint's beans are a {@code MultiTenantSftpClient} and a
         * {@code MultiTenantReactiveSftpClient}, still injectable as the plain client types.
         */
        private final Tenancy tenancy = new Tenancy();

        public Tenancy getTenancy() {
            return tenancy;
        }

        public static class Tenancy {

            /** Serve many tenants through this endpoint. */
            private boolean enabled;

            /**
             * Confine each tenant to a directory on this endpoint's connection: an absolute path
             * containing {@code {tenant}}, such as {@code /tenants/{tenant}}. Used when no
             * {@code SftpTenantProvider} bean decides instead.
             */
            private String rootDirectory;

            /**
             * The pool each tenant with a connection of its own gets — two sessions by default,
             * since there is one pool per tenant.
             */
            private final Pool pool = defaultTenantPool();

            private static Pool defaultTenantPool() {
                Pool pool = new Pool();
                pool.setMaxSize(2);
                return pool;
            }

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public String getRootDirectory() {
                return rootDirectory;
            }

            public void setRootDirectory(String rootDirectory) {
                this.rootDirectory = rootDirectory;
            }

            public Pool getPool() {
                return pool;
            }
        }

        public static class Pool {

            private int maxSize = 8;
            private Duration maxIdle = Duration.ofMinutes(5);
            private Duration acquireTimeout = Duration.ofSeconds(30);

            public int getMaxSize() {
                return maxSize;
            }

            public void setMaxSize(int maxSize) {
                this.maxSize = maxSize;
            }

            public Duration getMaxIdle() {
                return maxIdle;
            }

            public void setMaxIdle(Duration maxIdle) {
                this.maxIdle = maxIdle;
            }

            public Duration getAcquireTimeout() {
                return acquireTimeout;
            }

            public void setAcquireTimeout(Duration acquireTimeout) {
                this.acquireTimeout = acquireTimeout;
            }
        }

        public String getProvider() {
            return provider;
        }

        public void setProvider(String provider) {
            this.provider = provider;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Path getPrivateKey() {
            return privateKey;
        }

        public void setPrivateKey(Path privateKey) {
            this.privateKey = privateKey;
        }

        public String getPassphrase() {
            return passphrase;
        }

        public void setPassphrase(String passphrase) {
            this.passphrase = passphrase;
        }

        public HostKeyStrategy getHostKeyStrategy() {
            return hostKeyStrategy;
        }

        public void setHostKeyStrategy(HostKeyStrategy hostKeyStrategy) {
            this.hostKeyStrategy = hostKeyStrategy;
        }

        public Path getKnownHosts() {
            return knownHosts;
        }

        public void setKnownHosts(Path knownHosts) {
            this.knownHosts = knownHosts;
        }

        public Duration getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public Duration getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        /**
         * Redacted — endpoint config lands in startup logs and failure messages.
         */
        @Override
        public String toString() {
            return "Endpoint[" + username + "@" + host + ":" + port + ", provider=" + provider
                + ", auth=" + (privateKey != null ? "key" : "password") + "]";
        }
    }
}
