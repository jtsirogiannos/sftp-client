package io.github.jtsirogiannos.sftp.client.api;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * Everything needed to open a session, in JDK types only. Framework modules map *their* config onto
 * this type — Quarkus {@code @ConfigMapping}, Spring {@code @ConfigurationProperties} — so the
 * library never learns what a framework config property is (ROADMAP Phase 1).
 *
 * <pre>{@code
 * var config = SftpConnectionConfig.builder()
 *         .host("sftp.example.com")
 *         .username("svc")
 *         .password("hunter2".toCharArray())
 *         .build();
 * }</pre>
 *
 * <p><strong>The builder is the only way to construct one, and that is a versioning decision.</strong>
 * This was a record until its component count made the trap obvious: a public record's canonical
 * constructor is API, so the eleventh field — and the roadmap guarantees more fields — would break
 * every positional caller, source and binary. A final class with a private constructor keeps the
 * accessors and the builder exactly as they were, and new fields become new builder methods that
 * break nobody.
 *
 * <p><strong>Secrets are {@code char[]}, not {@code String}</strong>, so a caller can zero them after
 * connecting; interned strings live until GC and show up in heap dumps. Arrays are copied on the way
 * in and on the way out, so the array you pass is yours to wipe and the one you read is a throwaway.
 *
 * <p><strong>There is no value equality.</strong> Nothing in this library needs to compare configs,
 * and value-comparing secret arrays invites timing attacks — the previous record form documented the
 * same behaviour, which arrays gave it by accident. Identity equality is now the explicit contract.
 *
 * <p><strong>When both a password and a private key are set, both are offered.</strong> Each
 * provider registers both credentials and the server's authentication order decides which is used —
 * typically {@code publickey} before {@code password}. Neither is validated away, because offering a
 * fallback credential is a legitimate configuration.
 */
public final class SftpConnectionConfig {

    public static final int DEFAULT_PORT = 22;

    private final String host;
    private final int port;
    private final String username;
    private final char[] password;
    private final Path privateKey;
    private final char[] passphrase;
    private final HostKeyStrategy hostKeyStrategy;
    private final Path knownHosts;
    private final Duration connectTimeout;
    private final Duration requestTimeout;

    private SftpConnectionConfig(Builder builder) {
        this.host = Objects.requireNonNull(builder.host, "host");
        this.username = Objects.requireNonNull(builder.username, "username");
        this.hostKeyStrategy = Objects.requireNonNull(builder.hostKeyStrategy, "hostKeyStrategy");
        this.connectTimeout = Objects.requireNonNull(builder.connectTimeout, "connectTimeout");
        this.requestTimeout = Objects.requireNonNull(builder.requestTimeout, "requestTimeout");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (builder.port < 1 || builder.port > 65535) {
            throw new IllegalArgumentException("port out of range: " + builder.port);
        }
        if (builder.password == null && builder.privateKey == null) {
            throw new IllegalArgumentException(
                "no credentials: set password(...) or privateKey(...) on the builder");
        }
        if (builder.passphrase != null && builder.privateKey == null) {
            // A passphrase with nothing to unlock is a config typo that would otherwise surface
            // later, as an authentication failure with no hint of the cause.
            throw new IllegalArgumentException(
                "passphrase set but no privateKey: a passphrase protects a key file");
        }
        if (connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("connectTimeout must be positive: " + connectTimeout);
        }
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be positive: " + requestTimeout);
        }
        if (hostKeyStrategy != HostKeyStrategy.INSECURE_ACCEPT_ANY && builder.knownHosts == null) {
            throw new IllegalArgumentException(
                "hostKeyStrategy " + hostKeyStrategy + " needs a knownHosts file");
        }
        this.port = builder.port;
        this.privateKey = builder.privateKey;
        this.knownHosts = builder.knownHosts;
        // Defensive copy in: the caller may wipe their array the moment build() returns. (The
        // builder already cloned once on the way in; this clone keeps the config's copy private
        // even if the builder is reused.)
        this.password = builder.password == null ? null : builder.password.clone();
        this.passphrase = builder.passphrase == null ? null : builder.passphrase.clone();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String username() {
        return username;
    }

    /** Returns a copy; wipe it when done. Null when authenticating by key. */
    public char[] password() {
        return password == null ? null : password.clone();
    }

    public Path privateKey() {
        return privateKey;
    }

    /** Returns a copy; wipe it when done. Null for an unencrypted or absent key. */
    public char[] passphrase() {
        return passphrase == null ? null : passphrase.clone();
    }

    public HostKeyStrategy hostKeyStrategy() {
        return hostKeyStrategy;
    }

    public Path knownHosts() {
        return knownHosts;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    /**
     * Encodes a secret to UTF-8 bytes without an intermediate {@link String}.
     *
     * <p>{@code new String(chars).getBytes()} would be shorter and wrong: the String copy is
     * immortal until GC and defeats every {@code wipe} call around it, which is the whole reason
     * this type holds secrets as {@code char[]}. The encoder's scratch buffer is wiped here; the
     * caller owns wiping the returned array and the input.
     *
     * @return a UTF-8 encoding the caller must {@link #wipe(byte[])} after use
     */
    public static byte[] encodeSecret(char[] secret) {
        java.nio.ByteBuffer encoded = java.nio.charset.StandardCharsets.UTF_8
            .encode(java.nio.CharBuffer.wrap(secret));
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        if (encoded.hasArray()) {
            Arrays.fill(encoded.array(), (byte) 0);
        }
        return bytes;
    }

    /** Best-effort wipe; the byte twin of {@link #wipe(char[])}. */
    public static void wipe(byte[] secret) {
        if (secret != null) {
            Arrays.fill(secret, (byte) 0);
        }
    }

    /**
     * Best-effort wipe of a secret array. Overwrites in place; safe with null.
     */
    public static void wipe(char[] secret) {
        if (secret != null) {
            Arrays.fill(secret, '\0');
        }
    }

    /**
     * Redacted on purpose — configs end up in log lines and exception messages.
     */
    @Override
    public String toString() {
        return "SftpConnectionConfig[" + username + "@" + host + ":" + port
            + ", auth=" + (privateKey != null ? "key" : "password")
            + ", hostKeyStrategy=" + hostKeyStrategy + "]";
    }

    /**
     * Mutable builder; not thread-safe. One instance per config.
     */
    public static final class Builder {

        private String host;
        private int port = DEFAULT_PORT;
        private String username;
        private char[] password;
        private Path privateKey;
        private char[] passphrase;
        private HostKeyStrategy hostKeyStrategy = HostKeyStrategy.STRICT;
        private Path knownHosts = defaultKnownHosts();
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);

        private Builder() {
        }

        private static Path defaultKnownHosts() {
            String home = System.getProperty("user.home");
            return home == null ? null : Path.of(home, ".ssh", "known_hosts");
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(char[] password) {
            this.password = password == null ? null : password.clone();
            return this;
        }

        public Builder privateKey(Path privateKey) {
            this.privateKey = privateKey;
            return this;
        }

        public Builder passphrase(char[] passphrase) {
            this.passphrase = passphrase == null ? null : passphrase.clone();
            return this;
        }

        public Builder hostKeyStrategy(HostKeyStrategy hostKeyStrategy) {
            this.hostKeyStrategy = hostKeyStrategy;
            return this;
        }

        public Builder knownHosts(Path knownHosts) {
            this.knownHosts = knownHosts;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public SftpConnectionConfig build() {
            return new SftpConnectionConfig(this);
        }
    }
}
