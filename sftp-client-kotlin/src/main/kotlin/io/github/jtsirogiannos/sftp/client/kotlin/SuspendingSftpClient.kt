package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient
import io.github.jtsirogiannos.sftp.client.api.RemoteFile
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities
import io.github.jtsirogiannos.sftp.client.spi.SftpSession
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
class SuspendingSftpClient(
    /**
     * The neutral client underneath, exposed on purpose: this wrapper tracks the reactive surface
     * by hand, and the day it lags — as it once did, by thirteen operations — the caller can reach
     * what is missing instead of holding a second reference themselves.
     */
    val underlying: ReactiveSftpClient,
) : AutoCloseable {

    private val delegate: ReactiveSftpClient get() = underlying

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

    /** Attributes without following symlinks — what a tree walk must use. */
    suspend fun lstat(path: String): RemoteFile = delegate.lstat(path).await()

    suspend fun realpath(path: String): String = delegate.realpath(path).await()

    suspend fun symlink(linkPath: String, targetPath: String) {
        delegate.symlink(linkPath, targetPath).await()
    }

    suspend fun readlink(linkPath: String): String = delegate.readlink(linkPath).await()

    suspend fun hardlink(linkPath: String, targetPath: String) {
        delegate.hardlink(linkPath, targetPath).await()
    }

    suspend fun setAttributes(path: String, update: AttributeUpdate) {
        delegate.setAttributes(path, update).await()
    }

    suspend fun statvfs(path: String): FilesystemStats = delegate.statvfs(path).await()

    /** `mkdir -p`. An existing chain is not an error. */
    suspend fun mkdirs(path: String) {
        delegate.mkdirs(path).await()
    }

    /** Deletes a tree, removing symlinks as links, never following them. */
    suspend fun deleteRecursively(path: String): Int = delegate.deleteRecursively(path).await()

    suspend fun chmod(path: String, mode: Int) {
        delegate.chmod(path, mode).await()
    }

    /** Directory entries matching [filter], filtered client-side. */
    suspend fun list(path: String, filter: (RemoteFile) -> Boolean): List<RemoteFile> =
        delegate.list(path) { filter(it) }.await()

    suspend fun findFirst(path: String, match: (RemoteFile) -> Boolean): RemoteFile? =
        delegate.findFirst(path) { match(it) }.await().orElse(null)

    /**
     * Streams a remote file as backpressured chunks.
     *
     * Cold: nothing is transferred until the flow is collected, and abandoning the collection
     * cancels the transfer and discards the session.
     */
    fun download(remotePath: String): Flow<ByteBuffer> = delegate.download(remotePath).asFlow()

    /** As [download], starting at [fromOffset] — the resume primitive. */
    fun download(remotePath: String, fromOffset: Long): Flow<ByteBuffer> =
        delegate.download(remotePath, fromOffset).asFlow()

    /** Consumes [data] with backpressure and writes it, returning when the final flush lands. */
    suspend fun upload(remotePath: String, data: Flow<ByteBuffer>) {
        delegate.upload(remotePath, data.asPublisher()).await()
    }

    /** As [upload], with explicit write semantics — APPEND, or CREATE_NEW where supported. */
    suspend fun upload(remotePath: String, data: Flow<ByteBuffer>, mode: SftpSession.WriteMode) {
        delegate.upload(remotePath, data.asPublisher(), mode).await()
    }

    /**
     * Publishes by rename: a reader watching the destination never sees a partial file. Replacing
     * an existing file needs SFTP v5+ rename semantics and fails on a v3 server rather than
     * degrading — see the neutral client's documentation.
     */
    suspend fun uploadAtomically(remotePath: String, data: Flow<ByteBuffer>) {
        delegate.uploadAtomically(remotePath, data.asPublisher()).await()
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
