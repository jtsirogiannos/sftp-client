package io.github.jtsirogiannos.sftp.client.api;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * Everything needed to open a session, in JDK types only. Framework modules map *their* config onto
 * this record — Quarkus {@code @ConfigMapping}, Spring {@code @ConfigurationProperties} — so the
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
 * <p><strong>Secrets are {@code char[]}, not {@code String}</strong>, so a caller can zero them after
 * connecting; interned strings live until GC and show up in heap dumps. The record copies on the way
 * in and on the way out, so the array you pass is yours to wipe and the one you read is a throwaway.
 *
 * <p><strong>Equality is identity-based for the secret fields.</strong> Records generate
 * {@code equals} via {@code Objects.equals}, which on an array is reference comparison — two configs
 * with identical passwords are therefore unequal. That is deliberate: value-comparing secrets invites
 * timing attacks, and nothing in this library needs config equality.
 *
 * @param host            hostname or address of the SFTP server
 * @param port            TCP port; {@value #DEFAULT_PORT} unless the server says otherwise
 * @param username        the account to authenticate as
 * @param password        password credential, or {@code null} when authenticating by key. Copied on
 *                        the way in and out, so the caller's array stays theirs to wipe
 * @param privateKey      path to a private key file, or {@code null} when authenticating by password
 * @param passphrase      passphrase protecting {@code privateKey}, or {@code null} if it has none
 * @param hostKeyStrategy how the server's host key is trusted. There is no safe default, so this is
 *                        required — see {@link HostKeyStrategy}
 * @param knownHosts      known-hosts file backing {@link HostKeyStrategy#STRICT} and
 *                        {@link HostKeyStrategy#ACCEPT_NEW}, or {@code null} for the user default
 * @param connectTimeout  how long to wait for the TCP connect, key exchange and authentication
 * @param requestTimeout  how long to wait for a single SFTP request once connected
 */
public record SftpConnectionConfig(
    String host,
    int port,
    String username,
    char[] password,
    Path privateKey,
    char[] passphrase,
    HostKeyStrategy hostKeyStrategy,
    Path knownHosts,
    Duration connectTimeout,
    Duration requestTimeout) {

    public static final int DEFAULT_PORT = 22;

    public SftpConnectionConfig {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(username, "username");
        Objects.requireNonNull(hostKeyStrategy, "hostKeyStrategy");
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        if (password == null && privateKey == null) {
            throw new IllegalArgumentException(
                "no credentials: set password(...) or privateKey(...) on the builder");
        }
        if (hostKeyStrategy != HostKeyStrategy.INSECURE_ACCEPT_ANY && knownHosts == null) {
            throw new IllegalArgumentException(
                "hostKeyStrategy " + hostKeyStrategy + " needs a knownHosts file");
        }
        // Defensive copy in: the caller may wipe their array the moment build() returns.
        password = password == null ? null : password.clone();
        passphrase = passphrase == null ? null : passphrase.clone();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Encodes a secret to UTF-8 bytes without an intermediate {@link String}.
     *
     * <p>{@code new String(chars).getBytes()} would be shorter and wrong: the String copy is
     * immortal until GC and defeats every {@code wipe} call around it, which is the whole reason
     * this record holds secrets as {@code char[]}. The encoder's scratch buffer is wiped here; the
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
            java.util.Arrays.fill(encoded.array(), (byte) 0);
        }
        return bytes;
    }

    /** Best-effort wipe; the byte twin of {@link #wipe(char[])}. */
    public static void wipe(byte[] secret) {
        if (secret != null) {
            java.util.Arrays.fill(secret, (byte) 0);
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
     * Returns a copy; wipe it when done. Null when authenticating by key.
     */
    @Override
    public char[] password() {
        return password == null ? null : password.clone();
    }

    /**
     * Returns a copy; wipe it when done. Null for an unencrypted or absent key.
     */
    @Override
    public char[] passphrase() {
        return passphrase == null ? null : passphrase.clone();
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
            return new SftpConnectionConfig(host, port, username, password, privateKey, passphrase,
                hostKeyStrategy, knownHosts, connectTimeout, requestTimeout);
        }
    }
}
