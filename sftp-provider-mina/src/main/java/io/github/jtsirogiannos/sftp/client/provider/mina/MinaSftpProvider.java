package io.github.jtsirogiannos.sftp.client.provider.mina;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.config.hosts.HostConfigEntryResolver;
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier;
import org.apache.sshd.client.keyverifier.KnownHostsServerKeyVerifier;
import org.apache.sshd.client.keyverifier.RejectAllServerKeyVerifier;
import org.apache.sshd.client.keyverifier.ServerKeyVerifier;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.NamedResource;
import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.common.keyprovider.KeyIdentityProvider;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.time.Duration;

/**
 * Apache MINA SSHD backend.
 *
 * <p>Discovered by {@link java.util.ServiceLoader} through
 * {@code META-INF/services/io.github.jtsirogiannos.sftp.client.spi.SftpProvider}, so adding this jar to
 * the classpath is the entire installation step — the JDBC driver model the README describes.
 */
public final class MinaSftpProvider implements SftpProvider {

    private static final Logger log = LoggerFactory.getLogger(MinaSftpProvider.class);

    /**
     * SFTP v6 with pipelined transfers. {@code pipelinedStreaming = true} is an honest claim about
     * request overlap, not about threading: MINA keeps several read/write requests in flight so the
     * pulling worker rarely stalls, but a worker is still pulling. See {@link SftpCapabilities}.
     *
     * <p>{@code hardLinks} and {@code statvfs} are OpenSSH extensions whose availability depends on
     * the <em>server</em>, not on MINA. Reporting them as capabilities of the provider would be a
     * lie on half the servers you connect to, so they stay false until the SPI grows a
     * per-session capability query.
     */
    private static final SftpCapabilities CAPABILITIES = SftpCapabilities.builder()
            .maxSftpVersion(6)
            .pipelinedStreaming(true)
            .posixRename(true)
            .atomicCreateNew(true)
            .symlinks(true)
            .hardLinks(false)
            .statvfs(false)
            .build();

    @Override
    public String name() {
        return "mina";
    }

    @Override
    public SftpCapabilities capabilities() {
        return CAPABILITIES;
    }

    /**
     * Opens one SSH transport and one SFTP channel.
     *
     * <p>Each session gets its own {@link SshClient}, which means its own NIO thread pool — real
     * overhead, accepted deliberately because it makes ownership unambiguous: closing the session
     * closes everything it created, with no shared state to reason about. Now that sessions are
     * pooled the cost is paid once per pooled session rather than once per operation, which is what
     * makes it tolerable. Sharing one client across sessions would be the next optimisation, and it
     * belongs here rather than in the pool.
     */
    @Override
    public SftpSession connect(SftpConnectionConfig config) {
        SshClient client = harden(SshClient.setUpDefaultClient());
        client.setServerKeyVerifier(verifierFor(config));
        client.start();

        ClientSession session = null;
        try {
            Duration timeout = config.connectTimeout();
            session = client.connect(config.username(), config.host(), config.port())
                    .verify(timeout)
                    .getSession();

            authenticate(session, config);
            session.auth().verify(timeout);

            SftpClient sftp = SftpClientFactory.instance().createSftpClient(session);
            // The negotiated version decides real behaviour (v3 has no atomic rename), so it is the
            // first thing worth knowing when a capability-dependent operation misbehaves.
            log.debug("Connected to {}@{}:{} using {} auth; negotiated SFTP v{}", config.username(),
                    config.host(), config.port(), config.privateKey() != null ? "public-key" : "password",
                    sftp.getVersion());
            return new MinaSftpSession(client, session, sftp);
        } catch (IOException | RuntimeException e) {
            log.debug("Connection to {}@{}:{} failed ({}); host-key strategy was {}",
                    config.username(), config.host(), config.port(), e.toString(),
                    config.hostKeyStrategy());
            // Nothing is handed back, so nothing else can clean these up.
            closeQuietly(session);
            stopQuietly(client);
            throw new SftpException("could not connect to " + config.username() + "@"
                    + config.host() + ":" + config.port(), e);
        }
    }

    /**
     * Strips a default MINA client of everything it would otherwise inherit from the developer's SSH
     * environment, so a session connects to exactly what {@link SftpConnectionConfig} says and nothing
     * else. Package-private and returns the client so the connect path stays a one-liner and a test
     * can assert both guarantees without opening a socket.
     *
     * <p>Two inheritances are removed, for the same reason — a library must be hermetic:
     * <ul>
     *   <li><strong>~/.ssh identities.</strong> {@code setUpDefaultClient()} installs a watcher that
     *       loads local private keys and offers them during authentication. That authenticates with
     *       keys the caller never configured, advertises which keys exist on this host, and spends the
     *       server's limited auth attempts one rejected key at a time — enough of them and the server
     *       closes the session, which showed up as an intermittent "Session is being closed" on a
     *       perfectly valid password login.</li>
     *   <li><strong>~/.ssh/config.</strong> The default resolver reads that file on the first connect.
     *       An OpenSSH client config can silently rewrite the host, change the username, or route
     *       through a {@code ProxyJump} the caller never asked for, so a config addressed to
     *       "sftp.example.com" would quietly connect somewhere else. It also crashes outright on a
     *       {@code Match} section — "StreamCorruptedException: Currently not able to process Match
     *       sections" — which surfaced as an intermittent connect failure (intermittent only because
     *       MINA caches the failed parse, so only the first connect in the JVM threw).</li>
     * </ul>
     */
    static SshClient harden(SshClient client) {
        client.setKeyIdentityProvider(KeyIdentityProvider.EMPTY_KEYS_PROVIDER);
        client.setHostConfigEntryResolver(HostConfigEntryResolver.EMPTY);
        return client;
    }

    /** Key takes precedence over password when both are configured, matching the config record's docs. */
    private static void authenticate(ClientSession session, SftpConnectionConfig config) throws IOException {
        Path keyPath = config.privateKey();
        if (keyPath != null) {
            char[] passphrase = config.passphrase();
            try (InputStream in = Files.newInputStream(keyPath)) {
                FilePasswordProvider passwords = passphrase == null
                        ? FilePasswordProvider.EMPTY
                        : FilePasswordProvider.of(new String(passphrase));
                Iterable<KeyPair> keys = SecurityUtils.loadKeyPairIdentities(
                        session, NamedResource.ofName(keyPath.toString()), in, passwords);
                if (keys == null) {
                    throw new SftpException("no key pairs found in " + keyPath);
                }
                for (KeyPair key : keys) {
                    session.addPublicKeyIdentity(key);
                }
            } catch (java.security.GeneralSecurityException e) {
                throw new SftpException("could not load private key " + keyPath, e);
            } finally {
                SftpConnectionConfig.wipe(passphrase);
            }
            return;
        }

        char[] password = config.password();
        try {
            session.addPasswordIdentity(new String(password));
        } finally {
            // The String above is interned until GC regardless - MINA's API takes one - but the
            // array we were handed is ours to clear, and it is the copy the caller can't reach.
            SftpConnectionConfig.wipe(password);
        }
    }

    /**
     * Maps the neutral {@link io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy} onto MINA's
     * verifiers. Every provider must implement all three identically — a strategy that quietly means
     * something weaker on one backend is a security bug, which is why ROADMAP Phase 2 calls this out
     * as a parity requirement rather than a detail.
     */
    private static ServerKeyVerifier verifierFor(SftpConnectionConfig config) {
        Path knownHosts = config.knownHosts();
        return switch (config.hostKeyStrategy()) {
            // Unknown host -> reject. The delegate is what runs when the key is NOT already known.
            case STRICT -> new KnownHostsServerKeyVerifier(RejectAllServerKeyVerifier.INSTANCE, knownHosts);
            // Unknown host -> accept and record it (TOFU). A *changed* key still fails: that
            // decision lives in the acceptor, not the delegate, and the default refuses.
            case ACCEPT_NEW -> new KnownHostsServerKeyVerifier(AcceptAllServerKeyVerifier.INSTANCE, knownHosts);
            case INSECURE_ACCEPT_ANY -> AcceptAllServerKeyVerifier.INSTANCE;
        };
    }

    private static void closeQuietly(ClientSession session) {
        if (session != null) {
            try {
                session.close();
            } catch (Exception ignored) {
                // best effort during failure cleanup
            }
        }
    }

    private static void stopQuietly(SshClient client) {
        try {
            client.stop();
        } catch (RuntimeException ignored) {
            // best effort during failure cleanup
        }
    }
}
