package io.github.jtsirogiannos.sftp.client.tck;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;

import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A real SSH server, in-process, for the conformance suite to run against.
 *
 * <p>Using MINA SSHD's server for <em>both</em> providers is the deliberate choice from ROADMAP
 * Phase 2: the JSch provider gets tested against a genuine SSH implementation with no Docker and no
 * Testcontainers, so {@code mvn test} works on a laptop and in CI with nothing installed. The cost
 * is that MINA is on both sides of the JSch tests, so it cannot catch a bug that MINA's server and
 * MINA's client agree on — which is exactly why the ROADMAP suggests adding a Testcontainers
 * OpenSSH target later as a second, independent opinion.
 *
 * <p>Three details make this safe to run anywhere:
 * <ul>
 *   <li><strong>Loopback, port 0</strong> — the OS picks a free port, so parallel test runs never
 *       collide. Read the real one from {@link #port()} after {@link #start()}.
 *
 *       <p>Binding to {@value #LOOPBACK} rather than every interface is not only hygiene, though a
 *       test SSH server has no business being reachable from the network. It also prevents an
 *       intermittent connection failure. Port 0 on the wildcard address considers only wildcard
 *       bindings, so the OS can hand out a port another process already holds on {@code 127.0.0.1}
 *       specifically, and a desktop typically has a dozen or more listeners inside the ephemeral
 *       range (49152-65535 on macOS and Linux). The bind succeeds, but a client connecting to
 *       loopback is then routed to the <em>more specific</em> socket — the other process — and the
 *       handshake fails in whatever way that process happens to fail: a reset, a dropped connection,
 *       or a protocol error, none of which point at the real cause. Binding to loopback makes the
 *       allocator consider loopback bindings, so the collision cannot happen.</li>
 *   <li><strong>Host keys are generated in memory.</strong> {@code SimpleGeneratorHostKeyProvider}
 *       writes {@code hostkey.ser} to disk when given a path; the no-arg form keeps the key in the
 *       heap and it dies with the JVM. No key material ever touches the working tree.</li>
 *   <li><strong>The filesystem is a throwaway temp dir</strong> exposed through
 *       {@code VirtualFileSystemFactory}, so {@code "/upload"} to a client is
 *       {@code <tmp>/upload} on disk and a buggy provider cannot escape the sandbox.</li>
 * </ul>
 */
public final class EmbeddedSftpServer implements AutoCloseable {

    /** Loopback only: never reachable off the machine, and never shadowed by a wildcard listener. */
    public static final String LOOPBACK = "127.0.0.1";

    /**
     * A port band deliberately below the ephemeral range, which on macOS and Linux starts at 49152.
     *
     * <p>Port 0 would be the obvious choice and is the wrong one here, for two separate reasons that
     * both end in a test failing for something that is not the code's fault:
     *
     * <ul>
     *   <li>Desktops run a surprising number of services on ephemeral ports — this machine had
     *       eighteen. A port already bound on {@code 127.0.0.1} is still free in the wildcard space,
     *       so a wildcard bind gets it and loopback clients are routed to the other process
     *       instead.</li>
     *   <li>Client sockets draw their source port from the same range. A client connecting to a
     *       listener in that range can be handed the listener's own port as its source, connect to
     *       itself, and then read back its own SSH banner — which surfaces as a protocol parse
     *       error.</li>
     * </ul>
     *
     * <p>Binding low, on loopback, removes both. The band is scanned from a random offset so that
     * concurrent test JVMs — surefire forks, or several modules building at once — collide rarely and
     * recover immediately when they do.
     */
    private static final int PORT_BAND_START = 20_000;
    private static final int PORT_BAND_END = 32_000;
    private static final int BIND_ATTEMPTS = 60;

    public static final String USERNAME = "tck";
    public static final String PASSWORD = "tck-password";

    private SshServer server;
    private Path root;

    /** Starts the server and creates the {@code /upload} directory the suite expects. */
    public void start() {
        try {
            root = Files.createTempDirectory("sftp-tck-");
            Files.createDirectories(root.resolve("upload"));

            IOException lastFailure = null;
            for (int attempt = 0; attempt < BIND_ATTEMPTS; attempt++) {
                int port = PORT_BAND_START
                        + ThreadLocalRandom.current().nextInt(PORT_BAND_END - PORT_BAND_START);
                SshServer candidate = SshServer.setUpDefaultServer();
                candidate.setHost(LOOPBACK);
                candidate.setPort(port);
                candidate.setKeyPairProvider(new SimpleGeneratorHostKeyProvider());
                candidate.setPasswordAuthenticator((username, password, session) ->
                        USERNAME.equals(username) && PASSWORD.equals(password));
                candidate.setSubsystemFactories(List.of(new SftpSubsystemFactory()));
                candidate.setFileSystemFactory(new VirtualFileSystemFactory(root));
                try {
                    candidate.start();
                    server = candidate;
                    return;
                } catch (IOException taken) {
                    // Someone else holds it. Close what we half-built and try another port.
                    lastFailure = taken;
                    try {
                        candidate.stop(true);
                    } catch (IOException ignored) {
                        // never started; nothing to unwind
                    }
                }
            }
            throw new UncheckedIOException("could not bind an embedded SFTP server to "
                    + LOOPBACK + " in " + PORT_BAND_START + ".." + PORT_BAND_END + " after "
                    + BIND_ATTEMPTS + " attempts", lastFailure);
        } catch (IOException e) {
            throw new UncheckedIOException("could not start the embedded SFTP server", e);
        }
    }

    /** The port the OS assigned. Only meaningful after {@link #start()}. */
    public int port() {
        return server.getPort();
    }

    /** The temp directory backing the server, for tests that want to assert against real files. */
    public Path root() {
        return root;
    }

    /**
     * A config pointing at this server.
     *
     * <p>{@link HostKeyStrategy#INSECURE_ACCEPT_ANY} is correct here and nowhere else: the host key
     * is regenerated on every start, so there is nothing stable to pin and no known-hosts file to
     * check against. The uncomfortable name is doing its job.
     */
    public SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
                // The literal address, not "localhost": that name can resolve to ::1 first, and the
                // server is bound to the IPv4 loopback.
                .host(LOOPBACK)
                .port(port())
                .username(USERNAME)
                .password(PASSWORD.toCharArray())
                .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build();
    }

    /** Stops the server and deletes the temp filesystem. Safe to call twice. */
    @Override
    public void close() {
        if (server != null) {
            try {
                server.stop(true);
            } catch (IOException ignored) {
                // shutting down; nothing useful to do with a failure here
            }
            server = null;
        }
        if (root != null) {
            deleteRecursively(root);
            root = null;
        }
    }

    private static void deleteRecursively(Path path) {
        if (!Files.exists(path)) {
            return;
        }
        try (var walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best effort: it is a temp dir, and the OS will reap it
                }
            });
        } catch (IOException ignored) {
            // best effort
        }
    }
}
