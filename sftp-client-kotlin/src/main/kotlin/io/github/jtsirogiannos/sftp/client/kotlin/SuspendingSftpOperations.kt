package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations
import io.github.jtsirogiannos.sftp.client.api.RemoteFile
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities
import io.github.jtsirogiannos.sftp.client.spi.SftpSession
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.jdk9.asFlow
import kotlinx.coroutines.jdk9.asPublisher
import java.nio.ByteBuffer
import java.util.concurrent.CompletionStage

/**
 * Every operation, as a suspend function — the coroutine view of
 * [io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations].
 *
 * What a [SuspendingSftpClient] offers per call, and what [SuspendingSftpClient.withSession] hands
 * its block. Each call into the library is made with the coroutine's current sessions replayed onto
 * [context], which is how a call deep in a unit of work joins its session after any number of
 * dispatcher hops.
 */
open class SuspendingSftpOperations internal constructor(
    private val operations: ReactiveSftpOperations,
    internal val context: SftpSessionContext,
) {

    /** Where calls go; a client over many tenants picks the current tenant's here. */
    internal open suspend fun target(): ReactiveSftpOperations = operations

    /** Makes the call with the coroutine's sessions current, then waits for it. */
    internal suspend fun <T> call(operation: (ReactiveSftpOperations) -> CompletionStage<T>): T {
        val target = target()
        val bindings = currentBindings()
        return context.callWith(bindings) { operation(target) }.await()
    }

    suspend fun list(path: String): List<RemoteFile> = call { it.list(path) }

    suspend fun stat(path: String): RemoteFile = call { it.stat(path) }

    suspend fun exists(path: String): Boolean = call { it.exists(path) }

    suspend fun mkdir(path: String) {
        call { it.mkdir(path) }
    }

    suspend fun rename(from: String, to: String) {
        call { it.rename(from, to) }
    }

    suspend fun remove(path: String) {
        call { it.remove(path) }
    }

    /** Attributes without following symlinks — what a tree walk must use. */
    suspend fun lstat(path: String): RemoteFile = call { it.lstat(path) }

    suspend fun realpath(path: String): String = call { it.realpath(path) }

    suspend fun symlink(linkPath: String, targetPath: String) {
        call { it.symlink(linkPath, targetPath) }
    }

    suspend fun readlink(linkPath: String): String = call { it.readlink(linkPath) }

    suspend fun hardlink(linkPath: String, targetPath: String) {
        call { it.hardlink(linkPath, targetPath) }
    }

    suspend fun setAttributes(path: String, update: AttributeUpdate) {
        call { it.setAttributes(path, update) }
    }

    suspend fun statvfs(path: String): FilesystemStats = call { it.statvfs(path) }

    /** `mkdir -p`. An existing chain is not an error. */
    suspend fun mkdirs(path: String) {
        call { it.mkdirs(path) }
    }

    /** Deletes a tree, removing symlinks as links, never following them. */
    suspend fun deleteRecursively(path: String): Int = call { it.deleteRecursively(path) }

    suspend fun chmod(path: String, mode: Int) {
        call { it.chmod(path, mode) }
    }

    /** Directory entries matching [filter], filtered client-side. */
    suspend fun list(path: String, filter: (RemoteFile) -> Boolean): List<RemoteFile> =
        call { ops -> ops.list(path) { filter(it) } }

    suspend fun findFirst(path: String, match: (RemoteFile) -> Boolean): RemoteFile? =
        call { ops -> ops.findFirst(path) { match(it) } }.orElse(null)

    /**
     * Streams a remote file as backpressured chunks.
     *
     * Cold: nothing is transferred until the flow is collected, and abandoning the collection
     * cancels the transfer. On a client this always borrows its own connection, whatever session is
     * current — the flow is collected whenever its collector decides.
     */
    fun download(remotePath: String): Flow<ByteBuffer> = operations.download(remotePath).asFlow()

    /** As [download], starting at [fromOffset] — the resume primitive. */
    fun download(remotePath: String, fromOffset: Long): Flow<ByteBuffer> =
        operations.download(remotePath, fromOffset).asFlow()

    /** Consumes [data] with backpressure and writes it, returning when the final flush lands. */
    suspend fun upload(remotePath: String, data: Flow<ByteBuffer>) {
        call { it.upload(remotePath, data.asPublisher()) }
    }

    /** As [upload], with explicit write semantics — APPEND, or CREATE_NEW where supported. */
    suspend fun upload(remotePath: String, data: Flow<ByteBuffer>, mode: SftpSession.WriteMode) {
        call { it.upload(remotePath, data.asPublisher(), mode) }
    }

    /**
     * Publishes by rename: a reader watching the destination never sees a partial file. Replacing
     * an existing file needs SFTP v5+ rename semantics and fails on a v3 server rather than
     * degrading — see the neutral client's documentation.
     */
    suspend fun uploadAtomically(remotePath: String, data: Flow<ByteBuffer>) {
        call { it.uploadAtomically(remotePath, data.asPublisher()) }
    }

    fun capabilities(): SftpCapabilities = operations.capabilities()
}
