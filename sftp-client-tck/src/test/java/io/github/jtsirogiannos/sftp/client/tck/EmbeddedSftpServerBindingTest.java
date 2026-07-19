package io.github.jtsirogiannos.sftp.client.tck;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where the fixture binds, which decides whether the conformance suite is reliable.
 *
 * <p>Binding to port 0 on every interface makes {@code connect} fail roughly one time in several
 * hundred, with an error that points nowhere near the cause: the OS allocates port 0 from the
 * ephemeral range considering only wildcard bindings, so it can hand out a port another process
 * already holds on {@code 127.0.0.1}. A client connecting to loopback is then routed to that other
 * process, and the handshake fails as a reset, a dropped connection, or a protocol error depending
 * on what the other process does. A desktop typically has a dozen such listeners.
 *
 * <p>Two properties prevent it and both are asserted here, because a later "simplification" back to
 * {@code setPort(0)} would look harmless while reintroducing a failure that only shows up on a
 * machine with the wrong port in use, in a test that appears unrelated.
 */
class EmbeddedSftpServerBindingTest {

    /** Where every mainstream OS starts handing out ephemeral ports. */
    private static final int EPHEMERAL_RANGE_START = 49_152;

    @Test
    void bindsBelowTheEphemeralRange() {
        try (EmbeddedSftpServer server = new EmbeddedSftpServer()) {
            server.start();

            assertThat(server.port())
                .describedAs("a port in the ephemeral range can collide with another process's "
                    + "listener, and with the client's own source port")
                .isLessThan(EPHEMERAL_RANGE_START)
                .isGreaterThan(1024);
        }
    }

    @Test
    void bindsToLoopbackOnly() throws IOException {
        try (EmbeddedSftpServer server = new EmbeddedSftpServer()) {
            server.start();

            assertThat(server.config().host())
                .describedAs("the literal loopback address, not a name that may resolve to ::1")
                .isEqualTo("127.0.0.1");

            // If the server had taken the wildcard address, this bind would be refused. That it
            // succeeds is the evidence that the server holds loopback only — and therefore that it
            // cannot be shadowed by, or shadow, someone else's loopback listener.
            try (ServerSocket elsewhere = new ServerSocket(
                    server.port(), 1, InetAddress.getByName("0.0.0.0"))) {
                assertThat(elsewhere.isBound()).isTrue();
            }
        }
    }

    /** Two fixtures at once must never land on the same port, however the band is scanned. */
    @Test
    void concurrentServersGetDistinctPorts() {
        try (EmbeddedSftpServer first = new EmbeddedSftpServer();
             EmbeddedSftpServer second = new EmbeddedSftpServer()) {
            first.start();
            second.start();

            assertThat(first.port()).isNotEqualTo(second.port());
        }
    }
}
