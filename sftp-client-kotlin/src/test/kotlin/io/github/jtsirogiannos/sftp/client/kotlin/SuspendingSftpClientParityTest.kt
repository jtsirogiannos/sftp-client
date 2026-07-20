package io.github.jtsirogiannos.sftp.client.kotlin

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Every operation on the neutral reactive client must exist here by name — the tripwire that keeps
 * this hand-maintained wrapper from silently lagging, as it once did by thirteen operations.
 * Name-level rather than signature-level because the types change by design (CompletionStage to
 * suspend, Flow.Publisher to kotlinx Flow).
 */
class SuspendingSftpClientParityTest {

    @Test
    fun `covers every neutral operation by name`() {
        val wrapped = SuspendingSftpClient::class.java.declaredMethods.map { it.name }.toSet() +
            // `underlying` is the property accessor for the escape hatch, not an operation.
            setOf("getUnderlying")

        val missing = ReactiveSftpClient::class.java.methods
            .filter { !it.isSynthetic }
            .map { it.name }
            .distinct()
            .filterNot { wrapped.contains(it) }
            .sorted()

        assertTrue(missing.isEmpty()) {
            "operations on ReactiveSftpClient with no suspending wrapper: $missing"
        }
    }
}
