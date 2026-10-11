package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code openSession()}: one connection, held by the caller until close, with the one rule that
 * cannot be left to documentation — no other request while a read is in flight.
 */
public class OpenSessionTest {

    @Test
    public void holdsOneConnectionForEveryOperationUntilClosed() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            try (SftpClientSession session = client.openSession()) {
                session.mkdirs("/out/2026/10");
                session.upload("/out/2026/10/report.csv", text("a,b"));
                session.rename("/out/2026/10/report.csv", "/out/2026/10/final.csv");
                assertThat(session.exists("/out/2026/10/final.csv")).isTrue();

                assertThat(provider.sessions).describedAs("one connection for the lot").hasSize(1);
                assertThat(pool.liveSessions()).isEqualTo(1);
                assertThat(pool.idleSessions()).describedAs("checked out while open").isZero();
            }

            assertThat(pool.idleSessions()).describedAs("back in the pool after close").isEqualTo(1);
        }
    }

    @Test
    public void everyOpenSessionIsItsOwnConnection() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool);
             SftpClientSession first = client.openSession();
             SftpClientSession second = client.openSession()) {

            first.mkdir("/a");
            second.mkdir("/b");

            assertThat(provider.sessions).hasSize(2);
        }
    }

    @Test
    public void operationsAfterCloseFailClearlyAndCloseIsIdempotent() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            SftpClientSession session = client.openSession();
            session.close();
            session.close();

            assertThat(session.isOpen()).isFalse();
            assertThatThrownBy(() -> session.exists("/"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed");
            assertThat(pool.idleSessions()).describedAs("returned once, not twice").isEqualTo(1);
        }
    }

    @Test
    public void anOpenDownloadRefusesEveryOtherRequestUntilItIsClosed() throws Exception {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {
            client.upload("/src.txt", text("payload"));

            try (SftpClientSession session = client.openSession()) {
                InputStream in = session.download("/src.txt");

                assertThatThrownBy(() -> session.upload("/dst.txt", in))
                        .describedAs("the copy that corrupts a JSch channel")
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("still being read");
                assertThatThrownBy(() -> session.download("/src.txt"))
                        .describedAs("a second read resets the first one's queue on JSch")
                        .isInstanceOf(IllegalStateException.class);

                assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("payload");
                in.close();
                in.close(); // idempotent: must not count itself out twice

                assertThat(session.exists("/src.txt")).describedAs("usable again").isTrue();
            }
            assertThat(pool.idleSessions()).isEqualTo(1);
        }
    }

    @Test
    public void closingWithADownloadStillOpenDiscardsTheConnection() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {
            client.upload("/src.txt", text("payload"));

            SftpClientSession session = client.openSession();
            session.download("/src.txt");
            session.close();

            assertThat(pool.idleSessions()).isZero();
            assertThat(provider.last().closed).isTrue();
        }
    }

    /** The overlapping copy is still possible — on two sessions, which is what it needs. */
    @Test
    public void twoSessionsCopyOneFileToAnother() throws Exception {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {
            client.upload("/src.txt", text("payload"));

            try (SftpClientSession reader = client.openSession();
                 SftpClientSession writer = client.openSession();
                 InputStream in = reader.download("/src.txt")) {
                writer.upload("/dst.txt", in);
            }

            java.io.ByteArrayOutputStream copied = new java.io.ByteArrayOutputStream();
            client.download("/dst.txt", copied);
            assertThat(copied.toString(StandardCharsets.UTF_8)).isEqualTo("payload");
        }
    }

    private static InputStream text(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
