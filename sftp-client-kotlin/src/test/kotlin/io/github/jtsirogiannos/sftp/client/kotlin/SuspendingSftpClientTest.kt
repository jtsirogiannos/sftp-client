package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.core.DefaultReactiveSftpClient
import io.github.jtsirogiannos.sftp.client.core.SftpClients
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The coroutine facade against a real server.
 *
 * These are integration tests rather than unit tests on purpose. The interesting question is not
 * whether the wrapper delegates — it plainly does — but whether kotlinx's adapters line up with the
 * JDK types this library returns, and whether coroutine cancellation reaches all the way down into
 * the transfer. Neither can be answered by reading the code.
 */
class SuspendingSftpClientTest {

    private lateinit var server: EmbeddedSftpServer
    private lateinit var worker: ExecutorService

    @BeforeEach
    fun start() {
        server = EmbeddedSftpServer()
        server.start()
        worker = Executors.newFixedThreadPool(4)
    }

    @AfterEach
    fun stop() {
        worker.shutdownNow()
        server.close()
    }

    private fun client(): SuspendingSftpClient =
        SuspendingSftpClient(SftpClients.reactive("mina", server.config(), worker))

    @Test
    fun `suspend operations work against a real server`() = runBlocking {
        client().use { client ->
            client.mkdir("/upload/kotlin")

            assertTrue(client.exists("/upload/kotlin"))
            assertTrue(client.list("/upload").any { it.name() == "kotlin" })
            assertTrue(client.stat("/upload/kotlin").directory())
        }
    }

    @Test
    fun `upload takes a kotlinx Flow and download returns one`() = runBlocking {
        client().use { client ->
            val payload = "coroutines round trip".toByteArray(StandardCharsets.UTF_8)

            client.upload("/upload/kotlin.txt", flowOf(ByteBuffer.wrap(payload)))

            val readBack = client.download("/upload/kotlin.txt")
                .toList()
                .fold(ByteArray(0)) { acc, buffer ->
                    val chunk = ByteArray(buffer.remaining())
                    buffer.get(chunk)
                    acc + chunk
                }
            assertContentEquals(payload, readBack)
            assertEquals(payload.size.toLong(), client.stat("/upload/kotlin.txt").size())
        }
    }

    @Test
    fun `taking one chunk stops the download rather than draining it`() = runBlocking {
        client().use { client ->
            val payload = ByteArray(512 * 1024) // several chunks worth
            client.upload("/upload/cold.bin", flowOf(ByteBuffer.wrap(payload)))

            // take(1) cancels the upstream after the first element, which is the Flow-shaped way of
            // abandoning a transfer — the subscription is cancelled and the session discarded.
            val first = withTimeout(30_000) {
                client.download("/upload/cold.bin").take(1).toList()
            }

            assertEquals(1, first.size)
            assertTrue(first[0].remaining() > 0, "the first chunk should carry data")
            // The connection survives an abandoned transfer: the pool opens a fresh session.
            assertEquals(payload.size.toLong(), client.stat("/upload/cold.bin").size())
        }
    }

    /**
     * Cancelling the coroutine must leave no session checked out.
     *
     * The pool is built here rather than via [SftpClients] so its state is visible.
     *
     * What this test does **not** prove is *which* mechanism released the session. Cancelling the
     * coroutine also cancels the flow collection, so `asPublisher`'s subscription terminates and the
     * subscriber cleans up through the publisher path — this passes even with the CompletionStage
     * cancellation path removed, which was verified by running it against a build with that code
     * reverted. The mechanism-level proof lives in the core's `ReactiveCancellationTest`, which does
     * fail without it.
     *
     * It is still worth having: it is the guarantee a Kotlin caller actually depends on, end to end
     * through kotlinx's adapters, and it would catch a regression in either path.
     */
    @Test
    fun `cancelling the coroutine cancels the upload and frees the session`() = runBlocking {
        val provider = SftpClients.provider("mina")
        BoundedSftpSessionPool.blocking(
            provider, server.config(), 1,
            Duration.ofMinutes(5), Duration.ofSeconds(10)
        ).use { pool ->
            val client = SuspendingSftpClient(DefaultReactiveSftpClient(provider, pool, worker))

            // A source that keeps producing, so the upload is still in flight when we cancel.
            val slow = flow {
                repeat(2_000) {
                    emit(ByteBuffer.wrap(ByteArray(4096)))
                    delay(1)
                }
            }

            val job = launch(Dispatchers.IO) { client.upload("/upload/cancelled.bin", slow) }
            delay(300) // let the transfer get going
            assertTrue(pool.liveSessions() > 0, "the upload should have borrowed a session by now")

            job.cancel()
            job.join()

            // The discard happens on a worker thread, so give it a moment to land.
            val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos()
            while (pool.liveSessions() != 0 && System.nanoTime() < deadline) {
                delay(20)
            }
            assertEquals(0, pool.liveSessions(), "a cancelled upload must release its session")
            assertEquals(0, pool.idleSessions(), "a half-written upload must never be pooled")
        }
    }
}
