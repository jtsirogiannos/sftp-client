package io.github.jtsirogiannos.sftp.client.it;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Publishing by rename, against real servers.
 *
 * <p>The load-bearing test is {@link #aReaderNeverSeesAPartialFile}, and what makes it worth having
 * is that it fails for plain {@code upload} — {@link #plainUploadIsVisibleWhileStillPartial} pins
 * that down. Without the control, a test asserting "the file was correct afterwards" would pass for
 * both methods and prove nothing about atomicity at all.
 *
 * <p>Parameterised across the two backends because they land on opposite sides of the capability
 * that decides whether atomic replacement is possible: MINA negotiates v6 with this server and has
 * {@code posix-rename}, JSch is v3 and does not.
 */
class AtomicUploadIntegrationTest {

    private static EmbeddedSftpServer server;
    private static final AtomicInteger UNIQUE = new AtomicInteger();

    private static final byte[] OLD = "the previous version\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEW = newContent();

    @BeforeAll
    static void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
    }

    @AfterAll
    static void stopServer() {
        server.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void publishesToAPathThatDoesNotExistYet(String provider) {
        String path = uniquePath("create");
        try (SftpClient client = SftpClients.blocking(provider, server.config())) {
            client.uploadAtomically(path, new ByteArrayInputStream(NEW));

            assertThat(download(client, path)).isEqualTo(NEW);
            assertThat(leftoverPartFiles(client)).describedAs("the scratch file must not survive")
                .isZero();
        }
    }

    /**
     * The point of the whole feature: while the new bytes are in flight, an observer looking at the
     * destination sees the old file, never a truncated one.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina"})
    void aReaderNeverSeesAPartialFile(String provider) {
        String path = uniquePath("replace");
        try (SftpClient client = SftpClients.blocking(provider, server.config())) {
            client.upload(path, new ByteArrayInputStream(OLD));
            AtomicReference<byte[]> midTransfer = new AtomicReference<>();

            client.uploadAtomically(path, observing(client, path, midTransfer));

            assertThat(midTransfer.get())
                .describedAs("mid-upload the destination must still hold the previous version")
                .isEqualTo(OLD);
            assertThat(download(client, path)).isEqualTo(NEW);
        }
    }

    /** The control. Same observation, ordinary upload — and the destination is visibly truncated. */
    @ParameterizedTest
    @ValueSource(strings = {"mina"})
    void plainUploadIsVisibleWhileStillPartial(String provider) {
        String path = uniquePath("nonatomic");
        try (SftpClient client = SftpClients.blocking(provider, server.config())) {
            client.upload(path, new ByteArrayInputStream(OLD));
            AtomicReference<byte[]> midTransfer = new AtomicReference<>();

            client.upload(path, observing(client, path, midTransfer));

            assertThat(midTransfer.get())
                .describedAs("if this ever equals OLD or NEW, the observation is not landing "
                    + "mid-transfer and the atomic test above proves nothing")
                .isNotEqualTo(OLD)
                .isNotEqualTo(NEW);
        }
    }

    /**
     * A v3 server cannot rename over an existing file, and emulating it by deleting first would
     * reopen the exact window this method closes. It refuses instead.
     */
    @ParameterizedTest
    @ValueSource(strings = {"jsch"})
    void refusesToReplaceWhenTheServerCannotRenameAtomically(String provider) {
        String path = uniquePath("v3replace");
        try (SftpClient client = SftpClients.blocking(provider, server.config())) {
            client.upload(path, new ByteArrayInputStream(OLD));
            assertThat(client.capabilities().posixRename()).isFalse();

            assertThatThrownBy(() -> client.uploadAtomically(path, new ByteArrayInputStream(NEW)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("posixRename");

            assertThat(download(client, path))
                .describedAs("a refused publish must leave the destination untouched")
                .isEqualTo(OLD);
            assertThat(leftoverPartFiles(client)).isZero();
        }
    }

    /** A failure part-way through must not leave the scratch file behind, nor touch the target. */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void cleansUpTheScratchFileWhenTheUploadFails(String provider) {
        String path = uniquePath("failure");
        try (SftpClient client = SftpClients.blocking(provider, server.config())) {
            assertThatThrownBy(() -> client.uploadAtomically(path, failingStream()))
                .isInstanceOf(RuntimeException.class);

            assertThat(client.exists(path)).describedAs("nothing should have been published")
                .isFalse();
            assertThat(leftoverPartFiles(client))
                .describedAs("the .part file must be cleaned up after a failed upload").isZero();
        }
    }

    /**
     * The same default method, reached through {@code withSession}, where every operation it
     * composes runs on one pinned connection instead of borrowing per call.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void worksInsideAUnitOfWork(String provider) {
        String path = uniquePath("unitofwork");
        try (SftpClient client = SftpClients.blocking(provider, server.config())) {
            client.withSessionWithoutResult(
                work -> work.uploadAtomically(path, new ByteArrayInputStream(NEW)));

            assertThat(download(client, path)).isEqualTo(NEW);
            assertThat(leftoverPartFiles(client)).isZero();
        }
    }

    // --- helpers ---

    /** A stream that peeks at the destination once, part-way through being consumed. */
    private static InputStream observing(SftpClient client, String path,
            AtomicReference<byte[]> sink) {
        return new FilterInputStream(new ByteArrayInputStream(NEW)) {
            private int consumed;
            private boolean observed;

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                int read = super.read(b, off, Math.min(len, 8192));
                if (read > 0) {
                    consumed += read;
                    if (!observed && consumed > NEW.length / 2) {
                        observed = true;
                        sink.set(client.exists(path) ? download(client, path) : new byte[0]);
                    }
                }
                return read;
            }
        };
    }

    private static InputStream failingStream() {
        return new InputStream() {
            private int served;

            @Override
            public int read() throws IOException {
                if (served++ > 64) {
                    throw new IOException("simulated source failure");
                }
                return 'x';
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (served > 64) {
                    throw new IOException("simulated source failure");
                }
                served += len;
                return len;
            }
        };
    }

    private static byte[] download(SftpClient client, String path) {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        client.download(path, sink);
        return sink.toByteArray();
    }

    private static long leftoverPartFiles(SftpClient client) {
        return client.list("/upload").stream().filter(f -> f.name().endsWith(".part")).count();
    }

    private static String uniquePath(String tag) {
        return "/upload/atomic-" + tag + "-" + UNIQUE.incrementAndGet() + ".bin";
    }

    private static byte[] newContent() {
        byte[] content = new byte[512 * 1024];
        java.util.Arrays.fill(content, (byte) 'B');
        return content;
    }
}
