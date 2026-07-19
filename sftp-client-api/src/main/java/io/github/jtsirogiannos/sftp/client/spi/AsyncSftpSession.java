package io.github.jtsirogiannos.sftp.client.spi;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * A session for providers that can talk to the server without parking a thread.
 *
 * <p>The sibling of {@link SftpSession}, not a subtype of it. Both are {@link SftpSessionLifecycle},
 * which is all the pool ever needed; beyond that they share nothing, and deliberately so. A provider
 * implements the one it actually is.
 *
 * <p>Why this matters: {@link SftpSession} is blocking by construction, which is what makes writing a
 * provider a weekend job, but it means every reactive facade this project ships — Mutiny, Reactor,
 * coroutines — is ultimately a worker thread parked on a socket read. That is why the baseline
 * capability is called {@code pipelinedStreaming} and not "async". A provider that owns request-id
 * correlation can do better: an arriving {@code SSH_FXP_DATA} is pushed onto a {@link Flow.Publisher}
 * straight from the I/O thread, and downstream demand becomes the read window. Such a provider
 * implements this interface and sets {@link SftpCapabilities#asyncStreaming()}.
 *
 * <h2>What this interface deliberately does not do</h2>
 *
 * <p>It does not extend {@link SftpSession} and it offers no blocking convenience methods. Defaulting
 * the blocking half on top of the async one — joining each stage — would be wrong twice over. It
 * forces an async provider to implement nineteen blocking methods it has no use for, including
 * {@code openRead}/{@code openWrite}, which cannot be honestly satisfied without a bounded
 * {@code Publisher}-to-{@code InputStream} bridge. And a joining default blocks the calling thread,
 * aimed squarely at the event-loop callers this interface exists to serve.
 *
 * <p>If a caller wants a blocking view of an async provider, that adaptation belongs in one place in
 * the core rather than re-implemented by every provider author.
 *
 * <h2>Threading</h2>
 *
 * <p>Returned stages may complete on an I/O thread. Do not block in a dependent action, and do not
 * assume any particular completing thread.
 */
public interface AsyncSftpSession extends SftpSessionLifecycle {

    CompletionStage<List<RemoteFile>> listAsync(String path);

    CompletionStage<RemoteFile> statAsync(String path);

    CompletionStage<Boolean> existsAsync(String path);

    CompletionStage<Void> mkdirAsync(String path);

    CompletionStage<Void> renameAsync(String from, String to);

    CompletionStage<Void> removeAsync(String path);

    /**
     * The file's bytes, emitted as they arrive.
     *
     * <p>Demand is the read window: the publisher must not read further ahead than the subscriber has
     * requested. Cancellation must stop the transfer rather than drain it.
     *
     * @param offset where to start; 0 for the whole file
     */
    Flow.Publisher<ByteBuffer> readAsync(String path, long offset);

    /**
     * Consumes {@code data} into the remote file, completing when the write is durable.
     *
     * <p>The returned stage completing is the only signal that the upload landed — the same contract
     * the blocking {@link SftpSession#openWrite} has on {@code close()}.
     */
    CompletionStage<Void> writeAsync(String path, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode);
}
