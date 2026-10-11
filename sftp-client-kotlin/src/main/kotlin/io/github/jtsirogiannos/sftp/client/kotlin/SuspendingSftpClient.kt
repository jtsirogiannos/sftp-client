package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.api.MultiTenantReactiveSftpClient
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future

/**
 * Coroutine view of the neutral [ReactiveSftpClient].
 *
 * Thin on purpose, and thin in the same way [io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient]
 * lets the Mutiny and Reactor facades be thin: the interface speaks `CompletionStage` and
 * `java.util.concurrent.Flow.Publisher`, and kotlinx-coroutines already adapts both. Nothing here
 * knows about SFTP.
 *
 * A wrapper rather than extension functions, for two reasons. It matches what the Quarkus and Spring
 * modules do, and an extension named `list` would be shadowed by the member `list` it is trying to
 * improve on — Kotlin resolves members before extensions.
 *
 * ### Units of work
 *
 * [withSession] runs a block over one connection, and the session it opens is current for the
 * block's whole extent — carried in the coroutine context, so it survives suspensions and dispatcher
 * hops that a thread-local would not. Calls on this client from anywhere inside the block join it.
 *
 * ### Tenants
 *
 * Over a [MultiTenantReactiveSftpClient], calls act for the tenant named by [withTenant] in the
 * coroutine context, falling back to the client's own resolver when none is.
 *
 * ### Cancellation
 *
 * Cancelling the calling coroutine cancels the transfer: the upstream subscription is cancelled, the
 * remote file is closed, and the session is discarded rather than returned to the pool. One buffer
 * may still land, since demand is one chunk at a time and a write already handed to the stream
 * cannot be recalled.
 *
 * ### What `suspend` does not mean here
 *
 * It does not mean no thread is involved. Every transfer occupies a thread from the executor the
 * underlying client was built with, for its whole duration — neither backend offers a zero-thread
 * SFTP path, which is why the capability is called `pipelinedStreaming` rather than "async".
 * Concurrent transfers are bounded by that executor, not by how many coroutines you launch. Size it
 * for the transfers you expect to overlap.
 */
class SuspendingSftpClient(
    /**
     * The neutral client underneath, exposed on purpose: this wrapper tracks the reactive surface
     * by hand, and the day it lags — as it once did, by thirteen operations — the caller can reach
     * what is missing instead of holding a second reference themselves.
     */
    val underlying: ReactiveSftpClient,
) : SuspendingSftpOperations(underlying, underlying.sessionContext()), AutoCloseable {

    /** The current tenant's client, when the coroutine names one and there are tenants to name. */
    override suspend fun target(): ReactiveSftpOperations = client()

    private suspend fun client(): ReactiveSftpClient {
        val tenant = currentCoroutineContext()[CurrentSftpTenant]?.tenantId
        return if (tenant != null && underlying is MultiTenantReactiveSftpClient) {
            underlying.forTenant(tenant)
        } else {
            underlying
        }
    }

    /**
     * Runs [block] over one connection, with that session current for the block's whole extent.
     *
     * ```kotlin
     * client.withSession {
     *     mkdirs("/outbound/2026/10")
     *     upload("/outbound/2026/10/report.csv", data)
     *     archiver.archive()   // calls on `client` inside join this session
     * }
     * ```
     *
     * A nested `withSession` joins rather than borrowing another connection. Exceptions from the
     * block propagate unchanged; the connection goes back to the pool if it is still sound.
     */
    suspend fun <R> withSession(block: suspend SuspendingSftpOperations.() -> R): R {
        val client = client()
        val outer = currentBindings()
        return coroutineScope {
            val scope = this
            context.callWith(outer) {
                client.withSession<R> { operations ->
                    // Captured inside the neutral withSession, where its session has just been made
                    // current on this thread — then carried into the block's coroutine context.
                    val inner = context.capture()
                    scope.future(SftpSessionBindings(inner)) {
                        SuspendingSftpOperations(operations, context).block()
                    }
                }
            }.await()
        }
    }

    /**
     * Opens a session the caller manages; never current, never joined. Close it, or use
     * [SuspendingSftpSession.use].
     */
    suspend fun openSession(): SuspendingSftpSession =
        SuspendingSftpSession(client().openSession().await(), context)

    /** Where the underlying client looks for its current session. */
    fun sessionContext(): SftpSessionContext = context

    /**
     * Closes the underlying client and its connection pool.
     *
     * This wrapper owns the delegate it was given; the executor is not touched, because it was
     * handed in and is usually shared.
     */
    override fun close() = underlying.close()
}
