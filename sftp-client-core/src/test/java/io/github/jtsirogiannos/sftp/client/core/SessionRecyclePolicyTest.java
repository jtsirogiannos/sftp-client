package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * When a unit of work ends, its connection goes back to the pool if and only if the provider vouches
 * for it and nothing is still reading from it — and the caller's exceptions are the caller's.
 *
 * <p>The old rule was "discard if the block threw", which charged a reconnect for every business
 * exception and still had to trust {@code isValid()} on the happy path. These tests pin the rule that
 * replaced it, from both directions: what is recycled, and what is not.
 */
public class SessionRecyclePolicyTest {

    @Test
    public void theBlocksOwnExceptionReachesTheCallerUnchanged() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);
            IllegalStateException rejected = new IllegalStateException("order rejected");

            assertThatThrownBy(() -> client.withSessionWithoutResult(ops -> {
                throw rejected;
            })).isSameAs(rejected);
        }
    }

    @Test
    public void aHealthyConnectionIsRecycledEvenWhenTheBlockThrows() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            assertThatThrownBy(() -> client.withSessionWithoutResult(ops -> {
                ops.mkdir("/work");
                throw new IllegalStateException("business failure, nothing to do with SFTP");
            })).isInstanceOf(IllegalStateException.class);

            assertThat(pool.idleSessions()).isEqualTo(1);
            client.exists("/work");
            assertThat(provider.sessions)
                    .describedAs("the next call reuses the connection instead of reconnecting")
                    .hasSize(1);
        }
    }

    @Test
    public void aRecoverableFailureInsideTheBlockLeavesTheConnectionReusable() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            client.withSessionWithoutResult(ops -> assertThatThrownBy(() -> ops.stat("/missing"))
                    .isInstanceOfSatisfying(SftpException.class,
                            e -> assertThat(e.isNoSuchFile()).isTrue()));

            assertThat(pool.idleSessions()).isEqualTo(1);
            assertThat(provider.last().closed).isFalse();
        }
    }

    @Test
    public void aConnectionTheProviderNoLongerVouchesForIsDiscarded() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            client.withSessionWithoutResult(ops -> {
                ops.mkdir("/work");
                provider.last().broken = true; // what a provider reports after a desync
            });

            assertThat(pool.idleSessions()).isZero();
            assertThat(pool.liveSessions()).isZero();
            assertThat(provider.last().closed).isTrue();
        }
    }

    @Test
    public void aDownloadLeftOpenDiscardsTheSession() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);
            client.upload("/data.txt", text("payload"));

            client.withSessionWithoutResult(ops -> ops.download("/data.txt")); // never closed

            assertThat(pool.idleSessions())
                    .describedAs("unread bytes would be the next borrower's first reply")
                    .isZero();
            assertThat(provider.last().closed).isTrue();
        }
    }

    @Test
    public void aDownloadClosedInsideTheBlockLetsTheSessionBeRecycled() throws Exception {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);
            client.upload("/data.txt", text("payload")); // a per-call transfer: its session is discarded

            String read = client.withSession(ops -> {
                try (InputStream in = ops.download("/data.txt")) {
                    return new String(in.readAllBytes(), StandardCharsets.UTF_8);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });

            assertThat(read).isEqualTo("payload");
            assertThat(pool.idleSessions()).isEqualTo(1);
            assertThat(provider.last().closed).isFalse();
        }
    }

    /** Outside a block too: a visitor's exception is the caller's, not an SFTP failure. */
    @Test
    public void aVisitorsExceptionReachesThePerCallCallerUnchanged() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);
            client.mkdir("/walk");
            client.upload("/walk/a.txt", text("a"));
            IllegalArgumentException failure = new IllegalArgumentException("visitor failed");

            assertThatThrownBy(() -> client.visit("/walk", entry -> {
                throw failure;
            })).isSameAs(failure);
        }
    }

    private static InputStream text(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
