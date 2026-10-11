package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext
import kotlinx.coroutines.future.await

/**
 * An open session: every operation pinned to one connection until [close] — the coroutine view of
 * [ReactiveSftpClientSession], from [SuspendingSftpClient.openSession]. Operations run one at a
 * time in the order issued; a transfer has the connection to itself.
 */
class SuspendingSftpSession internal constructor(
    private val session: ReactiveSftpClientSession,
    context: SftpSessionContext,
) : SuspendingSftpOperations(session, context) {

    /** True until [close] has been called. */
    fun isOpen(): Boolean = session.isOpen

    /** Hands the connection back once the operations already issued have finished. Idempotent. */
    suspend fun close() {
        session.close().await()
    }
}

/**
 * Runs [block] with the session and closes it afterwards, whatever happens — `use` for a session
 * whose close suspends.
 */
suspend fun <R> SuspendingSftpSession.use(block: suspend (SuspendingSftpSession) -> R): R {
    try {
        return block(this)
    } finally {
        close()
    }
}
