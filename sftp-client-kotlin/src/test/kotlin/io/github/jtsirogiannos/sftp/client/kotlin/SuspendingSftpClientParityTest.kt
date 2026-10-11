package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Every method on the neutral reactive types must exist on its coroutine counterpart by name — the
 * tripwire that keeps these hand-maintained wrappers from silently lagging, as they once did by
 * thirteen operations. Name-level rather than signature-level because the types change by design
 * (CompletionStage to suspend, Flow.Publisher to kotlinx Flow).
 *
 * Checked type by type — operations, client, session — so that a method landing on the wrong one
 * is caught too.
 */
class SuspendingSftpClientParityTest {

    @Test
    fun `operations cover every neutral operation by name`() =
        assertCovered(ReactiveSftpOperations::class.java.methods.toList(), SuspendingSftpOperations::class.java)

    @Test
    fun `the client covers every neutral client method by name`() =
        assertCovered(ReactiveSftpClient::class.java.declaredMethods.toList(), SuspendingSftpClient::class.java,
            // `underlying` is the property accessor for the escape hatch, not an operation.
            extra = setOf("getUnderlying"))

    @Test
    fun `the session covers every neutral session method by name`() =
        assertCovered(ReactiveSftpClientSession::class.java.declaredMethods.toList(), SuspendingSftpSession::class.java)

    private fun assertCovered(
        neutral: List<java.lang.reflect.Method>,
        wrapper: Class<*>,
        extra: Set<String> = emptySet(),
    ) {
        val wrapped = wrapper.declaredMethods.map { it.name }.toSet() + extra
        val missing = neutral
            .filter { !it.isSynthetic && java.lang.reflect.Modifier.isPublic(it.modifiers) }
            .map { it.name }
            .distinct()
            .filterNot { wrapped.contains(it) }
            .sorted()

        assertTrue(missing.isEmpty()) {
            "neutral methods with no coroutine counterpart on ${wrapper.simpleName}: $missing"
        }
    }
}
