package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient
import io.github.jtsirogiannos.sftp.client.api.RemoteFile
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.jdk9.asFlow
import kotlinx.coroutines.jdk9.asPublisher
import java.nio.ByteBuffer

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
class SuspendingSftpClient(private val delegate: ReactiveSftpClient) : AutoCloseable {

    suspend fun list(path: String): List<RemoteFile> = delegate.list(path).await()

    suspend fun stat(path: String): RemoteFile = delegate.stat(path).await()

    suspend fun exists(path: String): Boolean = delegate.exists(path).await()

    suspend fun mkdir(path: String) {
        delegate.mkdir(path).await()
    }

    suspend fun rename(from: String, to: String) {
        delegate.rename(from, to).await()
    }

    suspend fun remove(path: String) {
        delegate.remove(path).await()
    }

    /**
     * Streams a remote file as backpressured chunks.
     *
     * Cold: nothing is transferred until the flow is collected, and abandoning the collection
     * cancels the transfer and discards the session.
     */
    fun download(remotePath: String): Flow<ByteBuffer> = delegate.download(remotePath).asFlow()

    /** Consumes [data] with backpressure and writes it, returning when the final flush lands. */
    suspend fun upload(remotePath: String, data: Flow<ByteBuffer>) {
        delegate.upload(remotePath, data.asPublisher()).await()
    }

    fun capabilities(): SftpCapabilities = delegate.capabilities()

    /**
     * Closes the underlying client and its connection pool.
     *
     * This wrapper owns the delegate it was given; the executor is not touched, because it was
     * handed in and is usually shared.
     */
    override fun close() = delegate.close()
}
