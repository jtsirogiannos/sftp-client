package io.github.jtsirogiannos.sftp.client.it;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code openSession()} over a real connection, for both network providers.
 *
 * <p>The payload is a megabyte so that JSch really does have READs in flight behind the open
 * stream when the second request is attempted — the situation in which interleaving corrupts its
 * channel, and the one the session therefore refuses.
 */
class OpenSessionIntegrationTest {

    private static final AtomicInteger UNIQUE = new AtomicInteger();
    private static EmbeddedSftpServer server;

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
    void refusesToInterleaveARequestWithAnOpenReadThenRecovers(String provider) throws Exception {
        byte[] payload = payload();
        String src = "/upload/guard-" + UNIQUE.incrementAndGet() + ".bin";
        try (SftpClient client = SftpClients.blocking(provider, server.config())) {
            client.upload(src, new ByteArrayInputStream(payload));

            try (SftpClientSession session = client.openSession()) {
                InputStream in = session.download(src);
                in.read(new byte[8192]); // reads are now in flight

                assertThatThrownBy(() -> session.stat(src))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("still being read");

                in.close();
                assertThat(session.stat(src).size())
                        .describedAs("the session is in step once the stream is closed")
                        .isEqualTo(payload.length);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void copiesAcrossTwoSessionsByteForByte(String provider) throws Exception {
        byte[] payload = payload();
        int id = UNIQUE.incrementAndGet();
        String src = "/upload/copy-src-" + id + ".bin";
        String dst = "/upload/copy-dst-" + id + ".bin";
        try (SftpClient client = SftpClients.blocking(provider, server.config())) {
            client.upload(src, new ByteArrayInputStream(payload));

            try (SftpClientSession reader = client.openSession();
                 SftpClientSession writer = client.openSession();
                 InputStream in = reader.download(src)) {
                writer.upload(dst, in);
            }

            ByteArrayOutputStream copied = new ByteArrayOutputStream();
            client.download(dst, copied);
            assertThat(copied.toByteArray()).isEqualTo(payload);
        }
    }

    private static byte[] payload() {
        byte[] payload = new byte[1024 * 1024];
        new Random(7L).nextBytes(payload);
        return payload;
    }
}
