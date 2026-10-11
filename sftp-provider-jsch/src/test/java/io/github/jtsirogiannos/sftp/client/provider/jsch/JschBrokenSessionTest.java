package io.github.jtsirogiannos.sftp.client.provider.jsch;

import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JSch reports a server's "failure" reply and an exception thrown partway through a request with the
 * same status, {@code SSH_FX_FAILURE}. Only the second can leave the channel out of step, and only
 * the second carries a cause — so that is what the session keys on.
 *
 * <p>The trigger here is deliberately mundane: JSch indexes into an empty path before sending
 * anything, and wraps the resulting {@code StringIndexOutOfBoundsException} exactly the way it wraps
 * an I/O error mid-reply. A genuine desync cannot be provoked on demand against a well-behaved server;
 * the classification it depends on can.
 */
class JschBrokenSessionTest {

    private EmbeddedSftpServer server;
    private SftpSession session;

    @BeforeEach
    void connect() {
        server = new EmbeddedSftpServer();
        server.start();
        session = new JschSftpProvider().connect(server.config());
    }

    @AfterEach
    void disconnect() {
        if (session != null) {
            session.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void aFailureCarryingACauseIsNotReportedAsAServerStatusAndInvalidatesTheSession() {
        assertThatThrownBy(() -> session.stat(""))
                .isInstanceOfSatisfying(SftpException.class, e ->
                        assertThat(e.status())
                                .describedAs("not a reply from the server, so no SSH_FX status")
                                .isEqualTo(SftpException.NO_STATUS));

        assertThat(session.isValid())
                .describedAs("the channel may be out of step; the pool must not hand it out again")
                .isFalse();
    }

    @Test
    void aServerStatusReplyLeavesTheSessionValid() {
        assertThatThrownBy(() -> session.stat("/upload/does-not-exist"))
                .isInstanceOfSatisfying(SftpException.class, e ->
                        assertThat(e.isNoSuchFile()).isTrue());

        assertThat(session.isValid()).isTrue();
    }

    @Test
    void literalEscapesExactlyWhatJschUnescapes() {
        assertThat(JschSftpSession.literal("/plain/path.txt")).isEqualTo("/plain/path.txt");
        assertThat(JschSftpSession.literal("/a*b?c\\d")).isEqualTo("/a\\*b\\?c\\\\d");
    }
}
