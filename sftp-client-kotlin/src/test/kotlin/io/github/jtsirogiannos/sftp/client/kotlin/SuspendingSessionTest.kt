package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.core.SftpClients
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer
import io.github.jtsirogiannos.sftp.client.tenancy.SftpTenantProvider
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Units of work in coroutines, against a real server. A coroutine changes threads at every
 * suspension, so the interesting claim — that a call made deep inside `withSession`, after hops
 * between dispatchers, still lands on the block's connection — needs a pool of one to prove: a call
 * that failed to join would wait for the connection the block holds, and time out.
 */
class SuspendingSessionTest {

    private lateinit var server: EmbeddedSftpServer
    private lateinit var worker: ExecutorService
    private val unique = AtomicInteger()

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

    private fun client(maxSize: Int = 1): SuspendingSftpClient = SuspendingSftpClient(
        SftpClients.builder("mina")
            .config(server.config())
            .pool(PoolSettings.defaults().withMaxSize(maxSize).withAcquireTimeout(Duration.ofMillis(500)))
            .reactive(worker))

    private fun scratch() = "/upload/kotlin-session-${unique.incrementAndGet()}"

    private fun text(content: String) = flowOf(ByteBuffer.wrap(content.toByteArray(StandardCharsets.UTF_8)))

    @Test
    fun `calls on the client deep inside the block join its session across dispatcher hops`() = runBlocking {
        client().use { client ->
            val dir = scratch()
            withTimeout(10_000) {
                client.withSession {
                    mkdir(dir)
                    withContext(Dispatchers.IO) { client.mkdir("$dir/from-io") }
                    delay(5)
                    withContext(Dispatchers.Default) { client.upload("$dir/from-default.txt", text("x")) }
                    assertTrue(client.exists("$dir/from-default.txt"))
                }
            }
            assertTrue(client.exists("$dir/from-io"))
        }
    }

    @Test
    fun `a nested withSession joins the outer one`() = runBlocking {
        client().use { client ->
            val dir = scratch()
            withTimeout(10_000) {
                client.withSession {
                    mkdir(dir)
                    client.withSession { mkdir("$dir/nested") }
                }
            }
            assertTrue(client.exists("$dir/nested"))
        }
    }

    @Test
    fun `the block's own exception propagates unchanged and the connection is reused`() = runBlocking {
        client().use { client ->
            val rejected = IllegalStateException("order rejected")
            val thrown = assertThrows<IllegalStateException> {
                client.withSession<Unit> { throw rejected }
            }
            // Not assertSame: with assertions on, kotlinx-coroutines' stack-trace recovery rethrows a
            // copy of an exception that crossed an await, keeping the original as its cause. What
            // matters — the caller's own type and message, not an SftpException — survives that.
            assertEquals("order rejected", thrown.message)
            assertTrue(generateSequence<Throwable>(thrown) { it.cause }.any { it === rejected })
            // A pool of one: this only completes if the connection was handed back.
            withTimeout(10_000) { assertTrue(client.exists("/upload")) }
        }
    }

    @Test
    fun `an opened session is the caller's to close`() = runBlocking {
        client(maxSize = 2).use { client ->
            val dir = scratch()
            val session = client.openSession()
            session.use {
                it.mkdirs("$dir/opened")
                assertTrue(it.isOpen())
            }
            assertFalse(session.isOpen())
            assertTrue(client.exists("$dir/opened"))
        }
    }

    @Test
    fun `withTenant confines calls to the tenant's directory`() = runBlocking {
        val root = scratch()
        SftpClients.blocking("mina", server.config()).use { setup ->
            setup.mkdirs("$root/acme")
            setup.mkdirs("$root/globex")
        }
        val multiTenant = SftpClients.multiTenant("mina", server.config())
            .tenants(SftpTenantProvider.rootDirectoryTemplate("$root/{tenant}"))
            .buildReactive(worker)
        SuspendingSftpClient(multiTenant).use { client ->
            withTenant("acme") {
                client.upload("/orders.csv", text("acme's"))
                client.withSession { mkdir("/in-session") }
            }
            withTenant("globex") { client.upload("/orders.csv", text("globex's")) }

            SftpClients.blocking("mina", server.config()).use { check ->
                assertTrue(check.exists("$root/acme/orders.csv"))
                assertTrue(check.exists("$root/acme/in-session"))
                assertTrue(check.exists("$root/globex/orders.csv"))
            }
            assertEquals(listOf("/in-session", "/orders.csv"),
                withTenant("acme") { client.list("/") }.map { it.path() }.sorted())
        }
    }
}
