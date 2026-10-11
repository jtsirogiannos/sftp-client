package io.github.jtsirogiannos.sftp.client.api;

import java.util.concurrent.CompletionStage;

/**
 * An open reactive session: every operation, pinned to one connection until {@link #close()}.
 *
 * <p>The non-blocking twin of {@link SftpClientSession}, from
 * {@link ReactiveSftpClient#openSession()} or handed to a {@link ReactiveSftpClient#withSession}
 * block. Not a transaction: SFTP has no rollback.
 *
 * <h2>Rules</h2>
 *
 * <ul>
 *   <li><strong>Operations run one at a time, in the order they were issued.</strong> A connection
 *       is single-owner, and reactive code fans out without trying — {@code Uni.combine},
 *       {@code flatMap}, two stages started back to back. Each operation waits for the one before
 *       it, so fanning out over one session is safe; it is just not parallel. For parallel work,
 *       use the client or open more sessions.</li>
 *   <li><strong>A transfer has the connection to itself.</strong> While a download is streaming or an
 *       upload is in flight, any other call on this session fails at once with
 *       {@link IllegalStateException} rather than queueing behind it: a call queued behind a
 *       download, and awaited inside that download's {@code onNext}, would wait forever. Transfers
 *       that must overlap belong on separate sessions.</li>
 *   <li><strong>Closing decides the connection's fate</strong>, as for the blocking session: back to
 *       the pool if the provider still vouches for it and no transfer on it failed or is still
 *       running, discarded otherwise. Operations already issued are allowed to finish first; a
 *       transfer still streaming is abandoned.</li>
 * </ul>
 */
public interface ReactiveSftpClientSession extends ReactiveSftpOperations {

    /** True until {@link #close()} has been called. */
    boolean isOpen();

    /**
     * Ends the session once the operations already issued have finished, and hands its connection
     * back or discards it. Idempotent: every call returns the same stage. Never fails. Operations
     * issued afterwards fail with {@link IllegalStateException}.
     */
    CompletionStage<Void> close();
}
