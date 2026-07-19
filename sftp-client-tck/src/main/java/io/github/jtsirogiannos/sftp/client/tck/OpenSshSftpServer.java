package io.github.jtsirogiannos.sftp.client.tck;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A real OpenSSH server in a container — the second, independent opinion the suite needs.
 *
 * <p>{@link EmbeddedSftpServer} puts Apache MINA SSHD on both sides of every test, including the
 * JSch ones. That cannot catch a bug the two MINA halves agree on, nor any place where MINA is more
 * permissive than the servers people actually run. OpenSSH is what is on the far end of most real
 * SFTP connections, so this is the fixture that decides whether the providers work in the world
 * rather than in a lab.
 *
 * <p>Expect it to disagree with the embedded server, and treat the disagreements as findings rather
 * than noise. The obvious one: OpenSSH's sftp-server speaks <strong>protocol version 3 only</strong>,
 * so a provider advertising v6 semantics is, against this server, advertising something it will not
 * get. That gap is exactly what a single-fixture suite hides.
 *
 * <p>Requires Docker. Callers should skip rather than fail when it is unavailable — see
 * {@link #isAvailable()}.
 */
public final class OpenSshSftpServer implements AutoCloseable {

    /** Kept identical to {@link EmbeddedSftpServer} so tests can swap fixtures without edits. */
    public static final String USERNAME = "tck";
    public static final String PASSWORD = "tck-password";

    private static final int SFTP_PORT = 22;

    /**
     * atmoz/sftp is OpenSSH with its {@code internal-sftp} subsystem and a chrooted user, which is
     * the common production shape. The command is {@code user:pass:[e]:[uid]:[gid]:[dirs]} — the
     * trailing {@code upload} creates the one writable directory inside the chroot, so {@code
     * /upload} means the same thing here as it does on the embedded server.
     */
    private static final String COMMAND = USERNAME + ":" + PASSWORD + ":::upload";

    private GenericContainer<?> container;

    /** True when Docker is reachable. Tests should assume this rather than fail without it. */
    public static boolean isAvailable() {
        try {
            return org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
        } catch (RuntimeException e) {
            return false;
        }
    }

    public void start() {
        container = new GenericContainer<>(DockerImageName.parse("atmoz/sftp:alpine"))
                .withExposedPorts(SFTP_PORT)
                .withCommand(COMMAND)
                .waitingFor(Wait.forListeningPort());
        container.start();
    }

    /** The mapped host port. Only meaningful after {@link #start()}. */
    public int port() {
        return container.getMappedPort(SFTP_PORT);
    }

    public String host() {
        return container.getHost();
    }

    /**
     * A config pointing at the container.
     *
     * <p>{@link HostKeyStrategy#INSECURE_ACCEPT_ANY} for the same reason as the embedded fixture:
     * the container generates fresh host keys on every start, so there is nothing stable to pin.
     */
    public SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
                .host(host())
                .port(port())
                .username(USERNAME)
                .password(PASSWORD.toCharArray())
                .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build();
    }

    @Override
    public void close() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }
}
