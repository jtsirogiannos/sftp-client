package io.github.jtsirogiannos.sftp.client.spi;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
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
 *
 * <p>Failures arrive as a <em>failed stage</em> rather than a thrown exception, carrying the same
 * {@link io.github.jtsirogiannos.sftp.client.api.SftpException} the blocking contract would produce,
 * with the server's {@code SSH_FX_*} status where there was one. A method here should not throw
 * synchronously; a caller composing stages has no way to catch that.
 */
public interface AsyncSftpSession extends SftpSessionLifecycle {

    /**
     * Directory entries, excluding {@code "."} and {@code ".."}. Each entry carries an absolute path
     * that can be passed straight back into another operation.
     */
    CompletionStage<List<RemoteFile>> listAsync(String path);

    /**
     * Attributes of a path, following symlinks. Fails with {@code SSH_FX_NO_SUCH_FILE} when the path
     * is absent, which is an ordinary answer and must not invalidate the session.
     */
    CompletionStage<RemoteFile> statAsync(String path);

    /**
     * Whether the path exists. Completes {@code false} rather than failing when it does not: a
     * missing file is the question, not an error.
     */
    CompletionStage<Boolean> existsAsync(String path);

    /**
     * Creates a single directory, without creating intermediate parents.
     */
    CompletionStage<Void> mkdirAsync(String path);

    /**
     * Moves {@code from} to {@code to}. Whether an occupied target is replaced or refused is decided
     * by {@link SftpCapabilities#posixRename()}, and the answer must match what the flag claims.
     */
    CompletionStage<Void> renameAsync(String from, String to);

    /**
     * Removes a file or an empty directory — see {@link SftpSession#remove} for the two-packet
     * subtlety that applies equally here.
     */
    CompletionStage<Void> removeAsync(String path);

    /**
     * Attributes of a path <strong>without following symlinks</strong>.
     *
     * <p>The counterpart to {@link #statAsync}, and the difference is not academic: anything that
     * walks a tree must use this, or a symlink is reported as its target and the walk follows it out
     * of the tree it was given.
     */
    CompletionStage<RemoteFile> lstatAsync(String path);

    /**
     * Resolves a path to its canonical absolute form, expanding {@code .}, {@code ..} and the
     * server's notion of the login directory.
     */
    CompletionStage<String> realpathAsync(String path);

    /**
     * Creates a symbolic link at {@code linkPath} pointing at {@code targetPath}.
     *
     * @throws UnsupportedOperationException if {@link SftpCapabilities#symlinks()} is false
     */
    CompletionStage<Void> symlinkAsync(String linkPath, String targetPath);

    /**
     * The target a symbolic link points at, unresolved.
     *
     * @throws UnsupportedOperationException if {@link SftpCapabilities#symlinks()} is false
     */
    CompletionStage<String> readlinkAsync(String linkPath);

    /**
     * Creates a hard link, an OpenSSH extension rather than base protocol.
     *
     * @throws UnsupportedOperationException if {@link SftpCapabilities#hardLinks()} is false
     */
    CompletionStage<Void> hardlinkAsync(String linkPath, String targetPath);

    /**
     * Applies a partial attribute update. Fields the update leaves unset must be left alone rather
     * than reset to a default — the whole point of {@link AttributeUpdate} being partial.
     */
    CompletionStage<Void> setAttributesAsync(String path, AttributeUpdate update);

    /**
     * Filesystem space and inode counts, an OpenSSH extension.
     *
     * @throws UnsupportedOperationException if {@link SftpCapabilities#statvfs()} is false
     */
    CompletionStage<FilesystemStats> statvfsAsync(String path);

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
